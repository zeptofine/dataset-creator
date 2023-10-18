import os
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np

from .configs.keyworded import fancy_repr
from .datarules.base_rules import File, Filter

# IDK Wtf to call these things other than scenarios


@fancy_repr
@dataclass
class OutputScenario:
    path: str
    filters: list[Filter]

    def run(self, img: np.ndarray, stat: os.stat_result):
        for f in self.filters:
            img = f.run(img=img)

        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(self.path, img)
        os.utime(self.path, (stat.st_atime, stat.st_mtime))


@fancy_repr
@dataclass
class FileScenario:
    file: File
    outputs: list[OutputScenario]

    def run(self):
        try:
            return self._run()
        except Exception as e:
            raise ScenarioFailureError(self) from e

    def _run(self):
        img: np.ndarray = cv2.imread(str(self.file.absolute_pth), cv2.IMREAD_UNCHANGED)
        stat: os.stat_result = os.stat(str(self.file.absolute_pth))
        for output in self.outputs:
            output.run(img, stat)
        return self


class ScenarioFailureError(Exception):
    def __init__(self, s: FileScenario):
        super().__init__(f"Scenario failed: {s}")
