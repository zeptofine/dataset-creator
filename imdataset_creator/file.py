import os
from dataclasses import dataclass
from pathlib import Path

from .configs.keyworded import fancy_repr


class MalleablePath(str):
    def __format__(self, format_spec):
        formats = format_spec.split(",")
        newfmt: MalleablePath = self
        for fmt in formats:
            if "=" in fmt:
                key, val = fmt.split("=")
                if key == "maxlen":
                    newfmt = MalleablePath(newfmt[: int(val)])
                else:
                    raise ValueError(f"Unknown format specifier: {key}")
            elif fmt == "underscores":
                newfmt = MalleablePath("_".join(self.split(" ")))
            elif fmt == "underscore_path":
                newfmt = MalleablePath("_".join(Path(self).parts))

        return str(newfmt)


@fancy_repr
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
