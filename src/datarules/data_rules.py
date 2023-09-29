from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Self

from dateutil import parser as timeparser
from polars import DataFrame, Datetime, col

from ..configs.configtypes import SpecialItemData
from .base_rules import Column, Comparable, FastComparable, Producer, Rule

if TYPE_CHECKING:
    from collections.abc import Callable

    from polars import Expr

STAT_TRACKED = ("st_size", "st_atime", "st_mtime", "st_ctime")


def stat2dict(result: os.stat_result) -> dict:
    return {k: getattr(result, k) for k in dir(result) if k in STAT_TRACKED}


def stat(pth):
    result = os.stat(pth)
    return stat2dict(result)


class FileInfoProducer(Producer):
    produces = MappingProxyType(
        {
            "mtime": Datetime("ms"),
            "atime": Datetime("ms"),
            "ctime": Datetime("ms"),
            "size": int,
        }
    )

    def __call__(self):
        return [
            {"stat": col("path").apply(stat)},
            {
                "mtime": col("stat").struct.field("st_mtime").apply(timestamp2datetime).cast(Datetime("ms")),
                "atime": col("stat").struct.field("st_ctime").apply(timestamp2datetime).cast(Datetime("ms")),
                "ctime": col("stat").struct.field("st_atime").apply(timestamp2datetime).cast(Datetime("ms")),
                "size": col("stat").struct.field("st_size"),
            },
        ]


def timestamp2datetime(mtime: int) -> datetime:
    return datetime.fromtimestamp(mtime)


def get_size(path: str):
    return os.stat(path).st_size


class StatRule(Rule):
    def __init__(
        self,
        before: str | datetime = "2100",
        after: str | datetime = "1980",
    ) -> None:
        super().__init__()
        self.requires = Column("mtime", Datetime("ms"))

        expr: Expr | bool = True

        self.before: datetime | None = None
        self.after: datetime | None = None
        if after is not None:
            if not isinstance(after, datetime):
                self.after = timeparser.parse(after)
            expr &= self.after < col("mtime")
        if before is not None:
            if not isinstance(before, datetime):
                self.before = timeparser.parse(before)
            expr &= self.before > col("mtime")
        if self.before is not None and self.after is not None and self.after > self.before:
            raise self.AgeError(self.after, self.before)

        self.comparer = FastComparable(expr)

    @classmethod
    def from_cfg(cls, cfg) -> Self:
        return cls(before=cfg["before"], after=cfg["after"])

    @classmethod
    def get_cfg(cls) -> dict:
        return {"before": "2100", "after": "1980"}

    class AgeError(timeparser.ParserError):
        def __init__(self, older, newer):
            super().__init__(f"{older} is older than {newer}")


class BlacknWhitelistData(SpecialItemData):
    whitelist: list[str]
    blacklist: list[str]


class BlacknWhitelistRule(Rule):
    def __init__(
        self,
        whitelist: list[str] | None = None,
        blacklist: list[str] | None = None,
    ) -> None:
        super().__init__()

        self.whitelist: list[str] | None = whitelist
        self.blacklist: list[str] | None = blacklist

        expr: Expr | bool = True

        if self.whitelist:
            for item in self.whitelist:
                expr &= col("path").str.contains(item)
        if self.blacklist:
            for item in self.blacklist:
                expr &= col("path").str.contains(item).is_not()
        self.comparer = FastComparable(expr)

    @classmethod
    def get_cfg(cls) -> BlacknWhitelistData:
        return {"whitelist": [], "blacklist": []}


class ResolvedRule(Rule):
    def __init__(self, use_full=False):
        super().__init__()
        self.use_full: bool = use_full
        self.comparer = Comparable(self.compare)

    def compare(self, selected: DataFrame, full: DataFrame) -> DataFrame:
        return selected.filter(
            col("path").is_in(
                {Path(p).resolve(): p for p in (full if self.use_full else selected).get_column("path")}.values()
            )
        )


class TotalLimitRule(Rule):
    def __init__(self, total=1000):
        super().__init__()
        self.total = total
        self.comparer = Comparable(self.compare)

    def compare(self, selected: DataFrame, _) -> DataFrame:
        return selected.head(self.total)
