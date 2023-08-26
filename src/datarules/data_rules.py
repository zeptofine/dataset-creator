from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from dateutil import parser as timeparser
from polars import Datetime, col

from util.file_list import get_file_list

from .base_rules import Column, DataRule, FastComparable

if TYPE_CHECKING:
    from collections.abc import Callable

    from polars import Expr


class StatRule(DataRule, FastComparable):
    config_keyword = "stats"

    def __init__(
        self,
        before: Annotated[str, "Only get items before this threshold"] = "2100",
        after: Annotated[str, "Only get items after this threshold"] = "1980",
    ) -> None:
        super().__init__()
        self.schema = (Column(self, "modifiedtime", Datetime, col("path").apply(StatRule.get_modified_time)),)
        self.before: datetime | None = None
        self.after: datetime | None = None
        if before is not None:
            self.before = timeparser.parse(before)
        if after is not None:
            self.after = timeparser.parse(after)
        if self.before is not None and self.after is not None and self.after > self.before:
            raise self.AgeError(self.after, self.before)

    @staticmethod
    def get_modified_time(path: str) -> datetime:
        return datetime.fromtimestamp(os.stat(path).st_mtime)  # noqa: PTH116

    def fast_comp(self) -> Expr | bool:
        param: Expr | bool = True
        if self.after:
            param &= self.after < col("modifiedtime")
        if self.before:
            param &= self.before > col("modifiedtime")
        return param

    class AgeError(timeparser.ParserError):
        def __init__(self, older, newer):
            super().__init__(f"{older} is older than {newer}")


class BlacknWhitelistRule(DataRule, FastComparable):
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

    def fast_comp(self) -> Expr | bool:
        args: Expr | bool = True
        if self.whitelist:
            for item in self.whitelist:
                if self.exclusive:
                    args &= col("path").str.contains(item)
                else:
                    args |= col("path").str.contains(item)

        if self.blacklist:
            for item in self.blacklist:
                args &= col("path").str.contains(item).is_not()
        return args

    @classmethod
    def get_cfg(cls):
        return {
            "whitelist": [],
            "blackist": [],
            "exclusive": False,
            "!#exclusive": "Only allow files that are valid by every whitelist string",
        }


class ExistingRule(DataRule, FastComparable):
    def __init__(self, folders: list[str] | list[Path], recurse_func: Callable) -> None:
        super().__init__()
        self.existing_list = ExistingRule._get_existing(*map(Path, folders))
        self.recurse_func: Callable[[Path], Path] = recurse_func

    def fast_comp(self) -> Expr | bool:
        return col("path").apply(self.intersection)

    def intersection(self, path):
        return self.recurse_func(path).with_suffix("") not in self.existing_list

    @staticmethod
    def _get_existing(*folders: Path) -> set:
        return set.intersection(
            *({file.relative_to(folder).with_suffix("") for file in get_file_list(folder, "*")} for folder in folders)
        )
