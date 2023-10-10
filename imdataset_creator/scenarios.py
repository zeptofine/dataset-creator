import os
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np

from .configs import FilterData
from .datarules.base_rules import File, Filter

# IDK Wtf to call these things other than scenarios


@dataclass
class OutputScenario:
    path: str
    filters: dict[Filter, FilterData]

    def run(self, img: np.ndarray, stat: os.stat_result):
        for f, kwargs in self.filters.items():
            img = f.run(img=img, **kwargs)

        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(self.path, img)
        os.utime(self.path, (stat.st_atime, stat.st_mtime))


@dataclass
class FileScenario:
    file: File
    outputs: list[OutputScenario]

    def run(self):
        img: np.ndarray = cv2.imread(str(self.file.absolute_pth), cv2.IMREAD_UNCHANGED)
        stat: os.stat_result = os.stat(str(self.file.absolute_pth))
        for output in self.outputs:
            output.run(img, stat)
        return self
