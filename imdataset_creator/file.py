from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from typing import Any, ClassVar

from typing_extensions import SupportsIndex


class InvalidFormatError(Exception):
    def __init__(self, disallowed: str):
        super().__init__(f"invalid format string. '{disallowed}' is not allowed.")


str_slice = re.compile(r"\[(?P<start>[-\d]*):(?P<stop>[-\d]*):?(?P<step>[-\d]*)\]")


class SafeFormatter(Formatter):
    def get_field(self, field_name: str, _: Sequence[Any], kwargs: Mapping[str, Any]) -> tuple[Any, Any]:
        # the goal is to make sure `property`s and indexing is still available, while dunders and things are not

        if "__" in field_name:
            raise InvalidFormatError("__")

        # check for int/str slice
        matches = list(str_slice.finditer(field_name))
        if len(matches) > 0:
            match = matches[0]
            variable = field_name[: match.start()]
            start = int(s) if (s := match.group("start")) else None
            stop = int(s) if (s := match.group("stop")) else None
            step = int(s) if (s := match.group("step")) else None

            return kwargs[variable][start:stop:step], variable

        return super().get_field(field_name, _, kwargs)


escaped_split = re.compile(r"[^\\],")
condition_fmt = re.compile(r"^(?P<prompt>[^\?:]+)\?(?P<true>(?:[^:])*):?(?P<false>.*)$")  # present?yes:no
replacement_fmt = re.compile(r"'(?P<from>[^']+)'='(?P<to>[^']*)'")


def condition_format(pth: str, match: re.Match) -> str:
    """
    Inline if condition. (if?then:else)
    """
    p = match.group("prompt")

    if (p := match.group("prompt")) in pth:
        return match.group("true") or p
    return match.group("false")


class MalleablePath(str):
    mpath_format_conditions: ClassVar[dict[re.Pattern, Callable[[str, re.Match], str]]] = {
        re.compile(r"^(?P<prompt>[^\?:]+)\?(?P<true>(?:[^:!])*):?(?P<false>.*)$"): (condition_format),
        re.compile(r"^'(?P<from>[^']+)'='(?P<to>[^']*)'$"): (  # replaces <from> to <to>
            lambda pth, m: pth.replace(m.group("from"), m.group("to"))
        ),
    }

    def __format__(self, format_spec: str):
        if not format_spec:
            return self

        formats = [s.replace(r"\,", ",") for s in escaped_split.split(format_spec)]
        newpth: str = str(self)
        for fmt in formats:
            if not fmt:
                continue

            patterns_used = False
            for pattern, func in self.mpath_format_conditions.items():
                if any(match := list(pattern.finditer(fmt))):
                    patterns_used = True
                    newpth = func(newpth, match[0])

            if fmt == "underscores":
                newpth = "_".join(newpth.split(" "))
            elif fmt == "underscore_parts":
                newpth = "_".join(Path(newpth).parts)

            elif not patterns_used:
                raise ValueError(f"Unknown format specifier: {fmt!r}")

        return str(newpth)

    def __getitem__(self, __key: SupportsIndex | slice) -> str:
        return MalleablePath(super().__getitem__(__key))


@dataclass(frozen=True)
class File:
    absolute_pth: MalleablePath
    src: MalleablePath
    relative_path: MalleablePath
    file: MalleablePath
    ext: str

    def to_dict(self) -> dict[str, str | MalleablePath]:
        return {
            "absolute_pth": self.absolute_pth,
            "src": self.src,
            "relative_path": self.relative_path,
            "file": self.file,
            "ext": self.ext,
        }

    @classmethod
    def from_src(cls, src: Path, pth: Path):
        return cls(
            absolute_pth=MalleablePath(pth.resolve()),
            src=MalleablePath(src),
            relative_path=MalleablePath(pth.relative_to(src).parent),
            file=MalleablePath(pth.stem),
            ext=pth.suffix[1:],
        )
