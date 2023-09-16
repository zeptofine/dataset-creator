from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated

from dateutil import parser as timeparser
from polars import DataFrame, Datetime, col

from util.file_list import get_file_list

from .base_rules import Column, Comparable, FastComparable, Producer, Rule

if TYPE_CHECKING:
    from collections.abc import Callable

    from polars import Expr

STAT_TRACKED = ("st_size", "st_atime", "st_mtime", "st_ctime")


def stat2dict(result: os.stat_result) -> dict:
    return {k: getattr(result, k) for k in dir(result) if k in STAT_TRACKED}


def stat(pth):
    result = os.stat(pth)  # noqa: PTH116
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


def get_modified_time(path: str) -> datetime:
    return datetime.fromtimestamp(os.stat(path).st_mtime)  # noqa: PTH116


def get_size(path: str):
    return os.stat(path).st_size  # noqa: PTH116


class StatRule(Rule):
    config_keyword = "stats"

    def __init__(
        self,
        before: Annotated[str, "Only get items before this threshold"] = "2100",
        after: Annotated[str, "Only get items after this threshold"] = "1980",
    ) -> None:
        super().__init__()
        self.requires = Column("mtime", Datetime("ms"))

        expr: Expr | bool = True

        self.before: datetime | None = None
        self.after: datetime | None = None
        if after is not None:
            self.after = timeparser.parse(after)
            expr &= self.after < col("mtime")
        if before is not None:
            self.before = timeparser.parse(before)
            expr &= self.before > col("mtime")
        if self.before is not None and self.after is not None and self.after > self.before:
            raise self.AgeError(self.after, self.before)

        self.comparer = FastComparable(expr)

    class AgeError(timeparser.ParserError):
        def __init__(self, older, newer):
            super().__init__(f"{older} is older than {newer}")


class BlacknWhitelistRule(Rule):
    config_keyword = "blackwhitelists"

    def __init__(
        self,
        whitelist: list[str] | None = None,
        blacklist: list[str] | None = None,
        exclusive: Annotated[bool, "Only allow files that are valid by every whitelist string"] = False,
    ) -> None:
        super().__init__()

        self.whitelist: list[str] | None = whitelist
        self.blacklist: list[str] | None = blacklist
        self.exclusive: bool = exclusive

        expr: Expr | bool = True

        if self.whitelist:
            for item in self.whitelist:
                if self.exclusive:
                    expr &= col("path").str.contains(item)
                else:
                    expr |= col("path").str.contains(item)
        if self.blacklist:
            for item in self.blacklist:
                expr &= col("path").str.contains(item).is_not()
        self.comparer = FastComparable(expr)

    @classmethod
    def get_cfg(cls):
        return {
            "whitelist": [],
            "blackist": [],
            "exclusive": False,
            "!#exclusive": "Only allow files that are valid by every whitelist string",
        }


class ExistingRule(Rule):
    def __init__(self, folders: list[str] | list[Path], recurse_func: Callable = lambda x: x) -> None:
        super().__init__()
        assert folders, "No folders given to check for existing files"
        self.existing_list = ExistingRule._get_existing(*map(Path, folders))
        self.recurse_func: Callable[[Path], Path] = recurse_func
        self.comparer = FastComparable(col("path").apply(self.intersection))

    def intersection(self, path):
        return self.recurse_func(path).with_suffix("") not in self.existing_list

    @staticmethod
    def _get_existing(*folders: Path) -> set:
        return set.intersection(
            *({file.relative_to(folder).with_suffix("") for file in get_file_list(folder, "*")} for folder in folders)
        )


class ResolvedRule(Rule):
    def __init__(self, use_full=False):
        super().__init__()
        self.use_full = use_full
        self.comparer = Comparable(self.compare)

    def compare(self, selected: DataFrame, full: DataFrame) -> DataFrame:
        return selected.filter(
            col("path").is_in(
                {Path(p).resolve(): p for p in (full if self.use_full else selected).get_column("path")}.values()
            )
        )


class TotalLimitRule(Rule):
    config_keyword = "limit"

    def __init__(self, total=1000):
        super().__init__()
        self.total = total
        self.comparer = Comparable(self.compare)

    def compare(self, selected: DataFrame, _) -> DataFrame:
        return selected.head(self.total)
