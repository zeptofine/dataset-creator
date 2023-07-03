from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dateutil import parser as timeparser
from polars import Datetime, col

from util.file_list import get_file_list

from .base_filters import DataFilter, FastComparable

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable
    from pathlib import Path

    from polars import DataFrame, Expr


class StatFilter(DataFilter, FastComparable):
    def __init__(self) -> None:
        super().__init__()
        self.column_schema = {"modifiedtime": Datetime}
        self.build_schema: dict[str, Expr] = {"modifiedtime": col("path").apply(StatFilter.get_modified_time)}
        self.config = (
            "stats",
            {
                "enabled": False,
                "before": "2040",
                "!#before": " only get items before this threshold",
                "after": "2010",
                "!#after": " only get items after this threshold",
            },
        )
        self.before: datetime | None = None
        self.after: datetime | None = None

    def populate_from_cfg(self, dct: dict[str, Any]):
        self.before = timeparser.parse(dct["before"])
        self.after = timeparser.parse(dct["after"])
        if self.after > self.before:
            raise timeparser.ParserError(f"{self.after} is older than {self.before}")

    @staticmethod
    def get_modified_time(path: str) -> datetime:
        return datetime.fromtimestamp(os.stat(path).st_mtime)

    def fast_comp(self) -> Expr | bool:
        param: Expr | bool = True
        if self.after:
            param &= self.after < col("modifiedtime)")
        if self.before:
            param &= self.before > col("modifiedtime")
        return param


class BlacknWhitelistFilter(DataFilter, FastComparable):
    def __init__(self) -> None:
        super().__init__()
        self.config = (
            "blackwhitelists",
            {
                "enabled": False,
                "whitelist": ["safe"],
                "!#whitelist": " files with these strings are filtered in",
                "all_whitelists_are_true": True,
                "!#all_whitelists_are_true": " allow files that are valid to __every__ whitelist string",
                "blacklist": ["explicit"],
                "!#blacklist": " items with these strings are filtered out",
            },
        )

        self.whitelist: list[str] | None = None
        self.blacklist: list[str] | None = None
        self.exclusive: bool = False

    def populate_from_cfg(self, dct: dict[str, Any]):
        self.exclusive = dct["all_whitelists_are_true"]
        return super().populate_from_cfg(dct)

    def compare(self, lst: Collection[Path], _: DataFrame) -> set:
        out: Iterable[Path] = lst
        if self.whitelist:
            out = self._whitelist(out, self.whitelist)
        if self.blacklist:
            out = self._blacklist(out, self.blacklist)
        return set(out)

    def fast_comp(self) -> Expr | bool:
        args: Expr | bool = True
        if self.whitelist:
            for item in self.whitelist:
                args &= col("path").str.contains(item)

        if self.blacklist:
            for item in self.blacklist:
                args &= col("path").str.contains(item).is_not()
        return args

    def _whitelist(self, imglist, whitelist) -> filter:
        return filter(lambda x: any(x in white for white in whitelist), imglist)

    def _blacklist(self, imglist, blacklist) -> filter:
        return filter(lambda x: all(x not in black for black in blacklist), imglist)


class ExistingFilter(DataFilter, FastComparable):
    def __init__(self, *folders, recurse_func: Callable) -> None:
        super().__init__()
        self.existing_list = ExistingFilter._get_existing(*folders)
        self.recurse_func: Callable[[Path], Path] = recurse_func

    def fast_comp(self) -> Expr | bool:
        return col("path").apply(
            lambda x: self.recurse_func(self.filedict[str(x)]).with_suffix("") not in self.existing_list
        )

    @staticmethod
    def _get_existing(*folders: Path) -> set:
        return set.intersection(
            *(
                {file.relative_to(folder).with_suffix("") for file in get_file_list((folder / "**" / "*"))}
                for folder in folders
            )
        )
