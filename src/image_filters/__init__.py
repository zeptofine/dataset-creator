import glob
import importlib
import os

__DIR_ORIGIN = os.path.dirname(__file__)
for file in glob.glob(__DIR_ORIGIN + "/*.py"):
    if "__" in file:
        continue
    importlib.import_module("." + os.path.splitext(os.path.relpath(file, __DIR_ORIGIN))[0], "src.image_filters")
