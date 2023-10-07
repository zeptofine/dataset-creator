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
            "absolute_pth": self.absolute_pth,
            "src": self.src,
            "relative_path": self.relative_path,
            "file": self.file,
            "ext": self.ext,
        }
