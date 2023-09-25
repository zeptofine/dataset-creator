from dataclasses import dataclass


@dataclass
class File:
    absolute_pth: str
    src: str
    relative_path: str
    file: str
    ext: str

    def to_dict(self):
        return {
            "absolute_pth": str(self.absolute_pth),
            "src": str(self.src),
            "relative_path": str(self.relative_path),
            "file": self.file,
            "ext": self.ext,
        }
