import os
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np

from src.configs import FilterData
from src.datarules.base_rules import File, Filter

# IDK Wtf to call these things other than scenarios


@dataclass
class OutputScenario:
    path: str
    filters: dict[Filter, FilterData]


@dataclass
class FileScenario:
    file: File
    outputs: list[OutputScenario]

    def run(self):
        img: np.ndarray
        original: np.ndarray
        original = cv2.imread(str(self.file.absolute_pth), cv2.IMREAD_UNCHANGED)

        mtime: os.stat_result = os.stat(str(self.file.absolute_pth))
        for output in self.outputs:
            img = original
            filters = output.filters.items()

            if filters:
                for filter_, kwargs in filters:
                    img = filter_.run(img=img, **kwargs)

            Path(output.path).parent.mkdir(parents=True, exist_ok=True)
            cv2.imwrite(output.path, img)
            os.utime(output.path, (mtime.st_atime, mtime.st_mtime))
        return self
