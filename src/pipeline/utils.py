import hashlib
import inspect
import sys
import time
from typing import get_origin, Union, get_args, Any, Type, get_type_hints


def get_base_type(tp):
    origin = get_origin(tp)
    if origin is None:
        # 处理Python内置和新语法中的基本类型
        if hasattr(tp, "__origin__"):
            return tp.__origin__
        return tp
    else:
        return origin


def is_union_subset(subset, superset):
    def get_union_args(tp):
        if tp is Union:
            return set()
        origin = get_origin(tp)
        if origin is Union:
            return set(get_args(tp))
        if hasattr(tp, "__args__"):
            return set(tp.__args__)
        return {tp}

    subset_args = get_union_args(subset)
    superset_args = get_union_args(superset)

    # 将类型转换为基础类型进行比较
    subset_args = {get_base_type(arg) for arg in subset_args}
    superset_args = {get_base_type(arg) for arg in superset_args}

    return superset_args.issubset(subset_args) or Any in superset_args


def get_class_signature(cls) -> str:
    source_code = ""
    for base in cls.__mro__:
        # Skip built-in classes
        if base.__module__ == "builtins":
            continue
        try:
            source_code += inspect.getsource(base)
        except TypeError:
            pass
    # for decorator scenario
    source_code += inspect.getsource(cls.generate)

    # hash
    return hashlib.md5(source_code.encode()).hexdigest()


def merge_dicts(*dicts):
    res = {}
    for d in dicts:
        res.update(d)
    return res


def get_time():
    return int(time.time() * 1000)


class RedirectStd:
    def __init__(self, out, err):
        self.out = out
        self.err = err
        self.stdout = sys.stdout
        self.stderr = sys.stderr

    def __enter__(self):
        sys.stdout = self.out
        sys.stderr = self.err
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self.stdout
        sys.stderr = self.stderr
        return False


class LoggerStream:
    def __init__(self, logger, level, prefix):
        self.logger = logger
        self.level = level
        self.prefix = prefix

    def write(self, message):
        self.logger.log(self.level, self.prefix + ": " + message)

    def flush(self):
        pass


def get_function_details(func) -> tuple[dict[str, Any], Type]:
    # 获取函数的类型提示
    type_hints = get_type_hints(func)

    # 获取函数的签名
    sig = inspect.signature(func)

    args = {}
    for param_name, param in sig.parameters.items():
        param_type = type_hints.get(param_name, Any)
        if param.default is inspect.Parameter.empty:
            args[param_name] = (param_type, ...)
        else:
            args[param_name] = (param_type, param.default)

    return args, type_hints.get("return", Any)
