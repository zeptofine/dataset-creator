from enum import Enum
from typing import TypeVar

T = TypeVar("T")


def listostr2listoenum(lst: list[str], enum: type[T]) -> list[T]:
    return [enum._member_map_[k] for k in lst]  # type: ignore
