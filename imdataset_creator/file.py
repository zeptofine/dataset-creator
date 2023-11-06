import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from typing import Any

from typing_extensions import SupportsIndex


class InvalidFormatError(Exception):
    def __init__(self, disallowed: str):
        super().__init__(f"invalid format string. '{disallowed}' is not allowed.")


str_slice = re.compile(r"\[(?P<start>[-\d]*):(?P<stop>[-\d]*):?(?P<step>[-\d]*)\]")
str_cond = re.compile(r"^(?P<prompt>[^\?:]+)\?(?P<true>(?:[^:])*):?(?P<false>.*)$")  # present?yes:no


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


class MalleablePath(str):
    def __format__(self, format_spec: str):
        formats = format_spec.split(",")
        newfmt: MalleablePath = self
        for fmt in formats:
            # if "=" in fmt:
            #     key, val = fmt.split("=")
            #     if key == "maxlen":
            #         newfmt = MalleablePath(newfmt[: int(val)])
            #     else:
            #         raise ValueError(f"Unknown format specifier: {key}")
            matches = list(str_cond.finditer(fmt))
            if matches:
                match = matches[0]
                if (p := match.group("prompt")) in newfmt:
                    newfmt = MalleablePath(t if (t := match.group("true")) else p)
                else:
                    newfmt = MalleablePath(match.group("false"))

            if fmt == "underscores":
                newfmt = MalleablePath("_".join(newfmt.split(" ")))
            elif fmt == "underscore_path":
                newfmt = MalleablePath("_".join(Path(newfmt).parts))
        return str(newfmt)

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
