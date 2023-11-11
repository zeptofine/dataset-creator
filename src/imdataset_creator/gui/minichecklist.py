from __future__ import annotations

from collections.abc import Collection

from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QGridLayout,
)


class MiniCheckList(QFrame):
    def __init__(self, items: Collection[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        grid = QGridLayout(self)
        grid.setVerticalSpacing(0)
        self.setLayout(grid)

        self.items: dict[str, QCheckBox] = {}
        for idx, item in enumerate(items):
            checkbox = QCheckBox(self)
            checkbox.setText(item)
            self.items[item] = checkbox
            grid.addWidget(checkbox, idx, 0)

    def disable_all(self):
        for item in self.items.values():
            item.setChecked(False)

    def get_config(self) -> dict[str, bool]:
        return {s: item.isChecked() for s, item in self.items.items()}

    def set_config(self, i: str, val: bool):
        self.items[i].setChecked(val)

    def get_enabled(self):
        return [i for i, item in self.items.items() if item.isChecked()]

    def update_items(self, dct: dict[str, bool]):
        for item, val in dct.items():
            self.items[item].setChecked(val)
