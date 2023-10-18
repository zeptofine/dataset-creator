import inspect
import sys
import textwrap
from dataclasses import dataclass
from enum import Enum, EnumType
from typing import Self

from .configtypes import ItemData, SpecialItemData


def _repr_indent(t):
    return textwrap.indent(t, "    ")


class Keyworded:
    @classmethod
    def cfg_kwd(cls):
        return cls.__name__

    @classmethod
    def from_cfg(cls, cfg) -> Self:
        return cls(**cfg)

    @classmethod
    def get_cfg(cls) -> ItemData | SpecialItemData:
        cfg = {}
        for key, val in list(inspect.signature(cls.__init__).parameters.items())[1:]:
            if issubclass(type(val.default), Enum):
                cfg[key] = val.default.value
            else:
                cfg[key] = val.default
            if val.annotation is not inspect._empty:
                annotation = eval(val.annotation, sys.modules[cls.__module__].__dict__)
                comment = Keyworded._obj_to_comment(annotation)
                if comment:
                    cfg[f"!#{key}"] = comment
        return cfg

    @staticmethod
    def _obj_to_comment(obj) -> str:
        if type(obj) is EnumType:
            return " | ".join(obj._member_map_.values())  # type: ignore
        if hasattr(obj, "__metadata__"):
            return str(obj.__metadata__[0])
        return ""

    def __repr__(self) -> str:
        attrs = ",\n".join([f"{key}={val!r}" for key, val in vars(self).items()])
        a = f"\n{_repr_indent(attrs)}\n" if attrs else ""
        return f"{self.__class__.__name__}({a})"
