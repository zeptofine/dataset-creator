from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class File:
    absolute_pth: str
    src: str
    relative_path: str
    file: str
    ext: str

    def to_dict(self):
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
            absolute_pth=str(pth),
            src=str(src),
            relative_path=str(pth.relative_to(src).parent),
            file=pth.stem,
            ext=pth.suffix[pth.suffix[0] == "." :],
        )
