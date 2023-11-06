import inspect
import sys
from enum import Enum, EnumType
from typing import Self

from .configtypes import ItemData, SpecialItemData


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
            if val.default is not val.empty:
                annotation = eval(val.annotation, sys.modules[cls.__module__].__dict__)
                comment = Keyworded._obj_to_comment(annotation)
                if comment:
                    cfg[f"!#{key}"] = comment
        return cfg

    @staticmethod
    def _obj_to_comment(obj) -> str:
        if type(obj) is EnumType:
            return " | ".join(obj.__members__.values())  # type: ignore
        if hasattr(obj, "__metadata__"):
            return str(obj.__metadata__[0])
        return ""

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}={val!r}" for key, val in vars(self).items() if all(k not in key for k in ("__",))
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"
