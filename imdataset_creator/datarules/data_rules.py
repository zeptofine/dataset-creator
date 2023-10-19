from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Self

from dateutil import parser as timeparser
from polars import DataFrame, Datetime, Expr, col

from ..configs.configtypes import SpecialItemData
from .base_rules import Comparable, DataColumn, FastComparable, Producer, ProducerSchema, Rule, combine_expr_conds

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

    def __call__(self) -> ProducerSchema:
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
        before: str | datetime | None = "2100",
        after: str | datetime | None = None,
    ) -> None:
        super().__init__()
        self.requires = DataColumn("mtime", Datetime("ms"))

        exprs: list[Expr] = []

        self.before: datetime | None = None
        self.after: datetime | None = None
        if after is not None:
            if not isinstance(after, datetime):
                self.after = timeparser.parse(after)
            exprs.append(self.after < col("mtime"))

        if before is not None:
            if not isinstance(before, datetime):
                self.before = timeparser.parse(before)
            exprs.append(self.before > col("mtime"))
        if self.before is not None and self.after is not None and self.after > self.before:
            raise self.AgeError(self.after, self.before)

        self.comparer = FastComparable(combine_expr_conds(exprs))

    @classmethod
    def from_cfg(cls, cfg) -> Self:
        return cls(before=cfg["before"], after=cfg["after"])

    @classmethod
    def get_cfg(cls) -> dict:
        return {"before": "2100", "after": "1980"}

    class AgeError(timeparser.ParserError):
        def __init__(self, older, newer):
            super().__init__(f"{older} is older than {newer}")


class EmptyListsError(ValueError):
    def __init__(self):
        super().__init__("whitelist and blacklist cannot both be empty")


class BlackWhitelistData(SpecialItemData):
    whitelist: list[str]
    blacklist: list[str]


class BlackWhitelistRule(Rule):
    def __init__(
        self,
        whitelist: list[str] | None = None,
        blacklist: list[str] | None = None,
    ) -> None:
        super().__init__()

        self.whitelist: list[str] | None = whitelist
        self.blacklist: list[str] | None = blacklist
        if not self.whitelist or self.blacklist:
            raise EmptyListsError()
        exprs: list[Expr] = []
        if self.whitelist:
            exprs.extend(col("path").str.contains(item) for item in self.whitelist)

        if self.blacklist:
            exprs.extend(col("path").str.contains(item).is_not() for item in self.blacklist)

        self.comparer = FastComparable(combine_expr_conds(exprs))

    @classmethod
    def get_cfg(cls) -> BlackWhitelistData:
        return {"whitelist": [], "blacklist": []}


class TotalLimitRule(Rule):
    def __init__(self, limit=1000):
        super().__init__()
        self.total = limit
        self.comparer = Comparable(self.compare)

    def compare(self, selected: DataFrame, _) -> DataFrame:
        return selected.head(self.total)
