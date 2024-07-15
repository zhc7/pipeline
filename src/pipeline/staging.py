import functools
import inspect
import logging
import os
import traceback
import typing
from typing import TypedDict, Any, Type, get_type_hints, Optional, Callable

from pipeline.datas import Data, StageMeta, EmptyData, AnyData, FailedData
from pipeline import global_logger
from pipeline.utils import (
    get_class_signature,
    get_function_details,
    merge_dicts,
    RedirectStd,
    LoggerStream,
    get_time,
)
from pydantic import create_model


def fields(*args, **kwargs) -> TypedDict:
    for arg in args:
        kwargs[arg] = Any
    return TypedDict("Anonymous", kwargs)


class Stage:
    def __init__(
        self,
        name: str | None = None,
        save_interval: int = 0,
        save_location: str = ".",
        retry: int = 3,
        logger: logging.Logger = global_logger,
        **kwargs,
    ):
        self.name = name or self.__class__.__name__
        self.save_interval = save_interval
        self.save_location = save_location
        self.retry = max(retry, 1)
        self.logger = logger
        self._kwargs = kwargs
        self._stages: dict[str, Stage] = {}
        self._buffer: list[Data] = []
        self.inp: Type[Data] | None = None
        self.out: Type[Data] | None = None
        self._mode: str | None = None
        self._generate_output_keys: list | None = None
        self._generate_input_signature: dict | None = None
        self._input_method: str | None = None
        self.removing = []
        self.meta_data = StageMeta(
            name=self.name, signature=get_class_signature(self.__class__)
        )

    def prepare_mp(self):
        self.inp = None
        self.out = None
        for s in self._stages.values():
            s.prepare_mp()

    def reinit(self):
        for s in self._stages.values():
            s.reinit()

        args, ret = get_function_details(self.generate)

        if not args:
            self.inp = EmptyData
        else:
            self.inp = create_model(
                self.name + "Input",
                **args,
                __base__=Data,
            )
        if isinstance(ret, Data):
            self.out = ret
            self._mode = "new"
            self._generate_output_keys = [
                key for key in self.out.model_fields.keys() if not key.endswith("_")
            ]
        elif isinstance(ret, typing._TypedDictMeta):
            ret_typing = get_type_hints(ret)
            additional = {
                key: (val, ...) for key, val in ret_typing.items() if val is not ...
            }
            self.removing = [key for key, val in ret_typing.items() if val is ...]
            types = {
                key: val
                for key, val in merge_dicts(args, additional).items()
                if key not in self.removing
            }
            self.out = create_model(
                self.name + "Output",
                **types,
                __base__=AnyData,
            )
            self._mode = "increment"
            self._generate_output_keys = list(additional.keys())
        else:
            self.out = AnyData
            self._mode = "increment"
            self._generate_output_keys = []

        self._generate_input_signature = get_function_details(self.generate)[0]
        if len(self._generate_input_signature) == 1 and issubclass(
            list(self._generate_input_signature.values())[0][0], Data
        ):
            self._input_method = "direct"
        else:
            self._input_method = "kwargs"
        self.logger.info(f"Stage {self.name} initialized")

    def generate(self, *args, **kwargs) -> dict[str, Any] | Data | TypedDict("Any", {}):
        raise NotImplementedError

    def __call__(self, inp: Data | dict | None = None, /, **kwargs) -> Data:
        # gen inp dict
        if isinstance(inp, dict):
            inp_dict = inp
        elif isinstance(inp, Data):
            inp_dict = inp.model_dump()
        else:
            inp_dict = kwargs

        if "failed_" in inp_dict:
            self.logger.info(f"Stage {self.name} received FailedData, skipping")
            return FailedData

        inp = self.inp.model_validate(inp_dict)

        # prepare params
        if self._input_method == "direct":
            params = (inp,)
            kw_params = {}
        elif self._input_method == "kwargs":
            kw_params = {}
            for name, t in self._generate_input_signature.items():
                kw_params[name] = inp[name]
            params = ()
        else:
            raise ValueError(f"Unknown input method: {self._input_method}")

        # core calling procedure
        for i in range(self.retry):
            try:
                with RedirectStd(
                    LoggerStream(self.logger, logging.INFO, self.name),
                    LoggerStream(self.logger, logging.ERROR, self.name),
                ):
                    out = self.generate(*params, **kw_params)
            except Exception as e:
                traceback.print_exc()
                if i == self.retry - 1:
                    self.logger.error(f"Stage {self.name} failed: {e}")
                    return FailedData
                else:
                    self.logger.warning(
                        f"Stage {self.name} failed: {e}, retrying: {i + 1}"
                    )
            else:
                break
        else:
            # Unreachable
            raise ValueError("Unreachable")

        # gen out dict
        if isinstance(out, Data):
            out_dict = out.model_dump()
        elif isinstance(out, dict):
            out_dict = out
        else:
            out_dict = {}
            if not isinstance(out, tuple):
                out = (out,)
            for i, key in enumerate(self._generate_output_keys):
                out_dict[key] = out[i]

        if "failed_" in out_dict:
            self.logger.info(f"Stage {self.name} returned FailedData")
            return FailedData

        # create output
        if self._mode == "increment":
            merged = merge_dicts(inp_dict, out_dict)
            for key in self.removing:
                merged.pop(key, None)
            out = self.out(**merged)
        elif self._mode == "new" and not isinstance(out, self.out):
            out = self.out(**out_dict)
        out.trace(self.meta_data, get_time())

        # auto save
        if self.save_interval > 0:
            self._buffer.append(out)
            if len(self._buffer) >= self.save_interval:
                self.dump(self._buffer, self.save_location)
                self.logger.info(
                    f"Stage {self.name} auto saved {len(self._buffer)} entries"
                )
                self._buffer.clear()

        self.logger.debug(f"Stage {self.name} finished")
        return out

    def add_stage(self, name: str, new_stage: "Stage"):
        self._stages[name] = new_stage

    def save_to(self, save_location: str):
        self.save_location = save_location
        for s in self._stages.values():
            s.save_to(save_location)

    def __setattr__(self, key: str, value: Any):
        if isinstance(value, Stage):
            self.add_stage(key, value)
        super().__setattr__(key, value)

    def dump(self, data: list[Data], location: str):
        """Save entries to a certain location. Default save in jsonlines format."""
        with open(os.path.join(location, self.name + ".jsonl"), "a") as f:
            for item in data:
                f.write(item.model_dump_json() + "\n")

    def load(self, location: str):
        """Load entries from a certain location. Default load in jsonlines format."""
        data = []
        with open(os.path.join(location, self.name + ".jsonl"), "r") as f:
            for line in f:
                data.append(self.out.model_validate_json(line))
        return data


@functools.wraps(Stage)
def stage(func: Optional[Callable] = None, /, **kwargs) -> Stage:
    if "name" not in kwargs:
        kwargs["name"] = func.__name__
    if func is None:
        return functools.partial(stage, **kwargs)

    class _Stage(Stage):
        def __init__(self, **_kwargs):
            super().__init__(**_kwargs)

        @functools.wraps(func)
        def generate(self, *args, **_kwargs):
            return func(*args, **_kwargs)

    # put self param in signature
    original_sig = inspect.signature(_Stage.generate)
    new_params = list(original_sig.parameters.values())
    new_params.insert(0, inspect.Parameter("self", inspect.Parameter.POSITIONAL_ONLY))
    _Stage.generate.__signature__ = original_sig.replace(parameters=new_params)

    return _Stage(**kwargs)
