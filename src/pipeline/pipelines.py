import json
import logging
import os
import sys
import time
import traceback
import typing
from typing import Callable
from logging.handlers import QueueHandler, QueueListener

if typing.TYPE_CHECKING:
    import multiprocessing as mp
else:
    import multiprocess as mp
from pipeline import global_logger
from pipeline.datas import EmptyData, Data
from pipeline.staging import Stage
from rich.console import Console
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import Progress, BarColumn


class Sequential(Stage):
    def __init__(self, stages: list[Stage | list[Stage]], retry: int = 1, **kwargs):
        super().__init__(stages=stages, retry=retry, **kwargs)
        for i, s in enumerate(stages):
            if isinstance(s, list):
                for j, ss in enumerate(s):
                    self.add_stage(f"stage_{i}_{j}", ss)
            else:
                self.add_stage(f"stage_{i}", s)
        self.stages = stages
        self.reinit()
        # validate stage model matches
        for i in range(len(stages) - 1):
            if isinstance(stages[i], list):
                # TODO: better solution, maybe not accept list at all
                continue
            if stages[i]._mode == "increment":
                continue
            mismatch = stages[i].out.not_fit(stages[i + 1].inp)
            if mismatch:
                raise ValueError(
                    f"Stage {i}'s out does not cover {i + 1}'s input: {mismatch}"
                )
        # set correct input output model
        self.inp = stages[0].inp if isinstance(stages[0], Stage) else stages[0][0].inp
        self.out = (
            stages[-1].out if isinstance(stages[-1], Stage) else stages[-1][0].out
        )

    def generate(self, inp: EmptyData) -> Data:
        for s in self.stages:
            inp = s(inp)
        return inp


class SizedQueue:
    def __init__(self, ctx=mp, *args, **kwargs):
        self.queue = ctx.Queue(*args, **kwargs)
        self.size = ctx.Value("i", 0)

    def put(self, *args, **kwargs):
        self.queue.put(*args, **kwargs)
        with self.size.get_lock():
            self.size.value += 1

    def get(self, *args, **kwargs):
        data = self.queue.get(*args, **kwargs)
        with self.size.get_lock():
            self.size.value -= 1
        return data

    def qsize(self):
        with self.size.get_lock():
            return self.size.value

    def put_nowait(self, *args, **kwargs):
        self.queue.put_nowait(*args, **kwargs)

    def empty(self):
        return self.queue.empty()


def default_initiator():
    while True:
        yield {}


class MPPipeline(Sequential):
    def __init__(
        self,
        stages: list[Stage | list[Stage]],
        max_queue_size: int = 100,
        multiple: int = 1,
        max_spawn: int = 15,
        initiator: Callable[[], typing.Iterable] = default_initiator,
        **kwargs,
    ):
        super().__init__(stages=stages, **kwargs)
        self.max_queue_size = max_queue_size
        self.multiple = multiple
        self.max_spawn = max_spawn
        self.initiator = initiator
        self.ctx = mp.get_context("spawn")
        self.queues = [
            SizedQueue(self.ctx, maxsize=max_queue_size)
            for _ in range(len(self.stages) + 1)
        ]
        self.processes: list[mp.Process] = []
        self.spawn_lock = mp.Lock()
        manager = self.ctx.Manager()
        self.statuses = manager.list()
        self.total = mp.Value("i", 0)
        for _ in range(len(stages)):
            self.statuses.append(manager.list())
        self.log_queue = SizedQueue(self.ctx)
        self.write_queue = SizedQueue(self.ctx)

    @staticmethod
    def _stage_wrapper(
        s: Stage,
        inp_queue: SizedQueue,
        out_queue: SizedQueue,
        log_queue: SizedQueue,
        write_queue: SizedQueue,
        status,
        index: int,
        is_first=False,
    ):
        name = s.name
        logger = logging.getLogger(f"{name}_{index}")
        logger.addHandler(QueueHandler(log_queue))
        logger.setLevel(logging.DEBUG)
        s.logger = logger

        class WriteQueue:
            def write(self, msg):
                write_queue.put(msg)

            def flush(self):
                pass

        sys.stdout = WriteQueue()
        sys.stderr = WriteQueue()
        s.reinit()
        while True:
            status[index] = "IDLE"
            inp = inp_queue.get()
            if inp is None:
                status[index] = "STOPPED"
                break
            status[index] = "RUNNING"
            try:
                out = s(inp).model_dump()
                if "failed_" in out:
                    continue
                status[index] = "QUEUING"
                out_queue.put(out)
            except:
                status[index] = "FAILED"
                raise

    def _final_saver(self, inp_queue: SizedQueue):
        buffer = []
        while True:
            data = inp_queue.get()
            if data is None:
                break
            if "failed_" in data:
                continue
            buffer.append(data)
            if len(buffer) > 0:
                try:
                    with open("final.jsonl", "a") as f:
                        for item in buffer:
                            f.write(json.dumps(item, ensure_ascii=False) + "\n")
                except:
                    traceback.print_exc()
                self.total.value += len(buffer)
                buffer.clear()

    def monitor(self):
        time_start = time.time()
        progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "[progress.completed]{task.completed} of {task.total}",
            "{task.fields[status]}",
        )

        colored_status = {
            "IDLE": "[green]ID[/green]",
            "RUNNING": "[yellow]UP[/yellow]",
            "FAILED": "[red]FA[/red]",
            "STOPPED": "[blue]ST[/blue]",
            "QUEUING": "[cyan]QU[/cyan]",
        }
        short_colored_status = {
            "IDLE": "[green]I[/green]",
            "RUNNING": "[yellow]U[/yellow]",
            "FAILED": "[red]F[/red]",
            "STOPPED": "[blue]S[/blue]",
            "QUEUING": "[cyan]Q[/cyan]",
        }

        tasks = []
        for i, q in enumerate(self.queues):
            tasks.append(
                progress.add_task(f"Queue {i}", total=self.max_queue_size, status="")
            )

        console = Console()
        listener = QueueListener(
            self.log_queue,
            RichHandler(console=console, level=logging.INFO),
            respect_handler_level=True,
        )
        listener.start()
        global_logger.info("monitor initialized")
        it = iter(self.initiator())

        # monitor
        with Live(progress, refresh_per_second=10, console=console):
            scores = [0] * len(self.stages)
            while True:
                # put seeds
                while self.queues[0].qsize() < self.max_queue_size:
                    try:
                        seed = next(it)
                        self.queues[0].put(seed)
                    except StopIteration:
                        self.queues[0].put(None)

                # redirected stdout
                with open("sub_process_log.txt", "a") as f:
                    while not self.write_queue.empty():
                        f.write(self.write_queue.get())

                # update progress
                for i, q in enumerate(self.queues):
                    itps = self.total.value / (time.time() - time_start)
                    speed = (
                        f"{itps:.2f} it/s"
                        if itps > 1 or itps == 0
                        else f"{1 / itps:.2f} s/it"
                    )
                    if i < len(self.stages):
                        if len(self.statuses[i]) <= 30:
                            status = " ".join(colored_status[s] for s in self.statuses[i])
                        elif len(self.statuses[i]) <= 50:
                            status = " ".join(short_colored_status[s] for s in self.statuses[i])
                        else:
                            status = "".join(short_colored_status[s] for s in self.statuses[i])
                    else:
                        status = f"Total: {self.total.value} " f"Average speed: {speed}"

                    progress.update(tasks[i], completed=q.qsize(), status=status)
                    if i < len(self.stages):
                        if q.qsize() >= self.max_queue_size / 2:
                            scores[i] += 1
                            if scores[i] > 20:
                                self.spawn(i)
                                # kill one before
                                self.reduce(i - 1)
                                scores[i] = 0
                        else:
                            scores[i] = 0

                time.sleep(0.2)

    def reduce(self, i):
        alive = sum([s not in ["FAILED", "STOPPED"] for s in self.statuses[i]])
        if i >= 0 and alive > 1:
            self.queues[i].put(None)

    def spawn(self, i):
        # get id
        if isinstance(self.stages[i], Stage):
            stages = (self.stages[i],)
        else:
            if len(self.statuses[i]) != 0:
                # already spawned
                return
            stages = self.stages[i]
        for stage in stages:
            with self.spawn_lock:
                for j, status in enumerate(self.statuses[i]):
                    if status == "STOPPED" or status == "FAILED":
                        self.statuses[i][j] = "IDLE"
                        break
                else:
                    j = len(self.statuses[i])
                    if j >= self.max_spawn:
                        return
                    self.statuses[i].append("IDLE")

            # spawn
            p = self.ctx.Process(
                target=self._stage_wrapper,
                args=(
                    stage,
                    self.queues[i],
                    self.queues[i + 1],
                    self.log_queue,
                    self.write_queue,
                    self.statuses[i],
                    j,
                    i == 0,
                ),
            )
            p.start()
            self.processes.append(p)

    def start(self, root: str):
        """One process for each stage"""
        os.makedirs(root, exist_ok=True)
        os.chdir(root)
        global_logger.log(logging.INFO, f"Pipeline started at {os.getcwd()}")
        self.prepare_mp()
        for i in range(len(self.stages)):
            for j in range(self.multiple):
                self.spawn(i)
        # final stage
        p = mp.Process(target=self._final_saver, args=(self.queues[-1],))
        p.start()
        self.processes.append(p)
        # monitor process
        global_logger.info("Starting monitor")
        p = mp.Process(target=self.monitor)
        p.start()
        self.processes.append(p)

    def stop(self):
        for queue in self.queues:
            for _ in range(self.multiple):
                queue.put(None)
        for p in self.processes:
            p.join()

    def wait(self):
        for p in self.processes:
            p.join()
