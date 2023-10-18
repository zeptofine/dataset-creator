# this is one of those words that look weird when you hear it a lot

import logging
import os
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

from .main_window import MainWindow, get_recent_files, save_recent_files

LOGGING_FORMAT = "%(levelname)s %(filename)s:%(lineno)d: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cfg_path")
    args = parser.parse_args()

    # check all recent files exist

    save_recent_files([file for file in get_recent_files() if os.path.exists(file)])

    app = QApplication([])
    central_window = MainWindow(Path(args.cfg_path)) if args.cfg_path else MainWindow()
    central_window.show()
    code = app.exec()
    sys.exit(code)


if __name__ == "__main__":
    main()
