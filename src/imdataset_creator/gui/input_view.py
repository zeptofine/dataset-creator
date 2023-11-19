from __future__ import annotations

import logging

from .. import Input
from .config_inputs import ItemDeclaration, ProceduralConfigList
from .settings_inputs import DirectoryInput, ItemSettings, MultilineInput

log = logging.getLogger()


DEFAULT_IMAGE_FORMATS = (
    ".webp",
    ".bmp",
    ".jpeg",
    ".jpg",
    ".png",
    ".tiff",
    ".tif",
)

InputView_ = ItemDeclaration[Input](
    "Input",
    Input,
    settings=ItemSettings(
        {
            "folder": DirectoryInput().label("Folder:"),
            "expressions": MultilineInput(
                default="\n".join(f"**/*{ext}" for ext in DEFAULT_IMAGE_FORMATS),
                is_list=True,
            ),
        }
    ),
)


def input_list(parent=None):
    return ProceduralConfigList(InputView_, parent=parent).label("Inputs")


# class GathererThread(QThread):
#     inputobj: Input
#     total = Signal(int)
#     files = Signal(list)
#     def run(self):
#         log.info(f"Starting search in: '{self.inputobj.folder}' with expressions: {self.inputobj.expressions}")
#         filelist = []
#         count = 0
#         self.total.emit(0)
#         emit_timer = time.time()
#         for file in self.inputobj.run():
#             count += 1
#             if (new_time := time.time()) > emit_timer + 0.2:
#                 self.total.emit(count)
#                 emit_timer = new_time
#             filelist.append(file)
#         log.info(f"Gathered {count} files from '{self.inputobj.folder}'")
#         self.total.emit(count)
#         self.files.emit(filelist)
