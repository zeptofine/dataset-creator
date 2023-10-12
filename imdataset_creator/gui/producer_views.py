from __future__ import annotations

from PySide6.QtWidgets import QComboBox, QLabel

from ..datarules import base_rules, data_rules, image_rules
from .frames import FlowItem, FlowList


class ProducerView(FlowItem):
    title = "Producer"
    movable = False

    bound_item: type[base_rules.Producer]

    def setup_widget(self):
        super().setup_widget()
        if self.desc:
            self.desc += "\n"
        self.desc += f"Produces: {set(self.bound_item.produces)}"
        self.description_widget.setText(self.desc)


class FileInfoProducerView(ProducerView):
    title = "File Info Producer"

    bound_item = data_rules.FileInfoProducer

    def get(self):
        super().get()
        return self.bound_item()


class ImShapeProducerView(ProducerView):
    title = "Image shape"
    bound_item = image_rules.ImShapeProducer

    def get(self):
        super().get()
        return self.bound_item()


class HashProducerView(ProducerView):
    title = "Hash Producer"
    desc = "gets a hash for the contents of an image"
    bound_item: type[image_rules.HashProducer] = image_rules.HashProducer
    needs_settings = True

    def configure_settings_group(self):
        self.hash_type = QComboBox()
        self.hash_type.addItems([*image_rules.HASHERS])
        self.group_grid.addWidget(QLabel("Hash type: ", self), 0, 0)
        self.group_grid.addWidget(self.hash_type, 0, 1)

    def reset_settings_group(self):
        self.hash_type.setCurrentIndex(0)

    def get_config(self):
        return {"hash_type": self.hash_type.currentText()}

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.hash_type.setCurrentText(cfg["hash_type"])
        return self

    def get(self):
        super().get()
        return self.bound_item(image_rules.HASHERS(self.hash_type.currentText()))


class ProducerList(FlowList):
    items: list[ProducerView]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__registered_by: dict[str, type[ProducerView]] = {}

    def add_item_to_menu(self, item: type[ProducerView]):
        self.add_menu.addAction(f"{item.title}: {set(item.bound_item.produces)}", lambda: self.initialize_item(item))

    def _register_item(self, item: type[ProducerView]):
        super()._register_item(item)
        for produces in item.bound_item.produces:
            self.__registered_by[produces] = item

    def registered_by(self, s: str):
        return self.__registered_by.get(s)
