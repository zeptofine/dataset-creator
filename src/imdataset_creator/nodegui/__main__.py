import logging
from collections.abc import Callable
from pprint import pformat

import qtpynodeeditor as ne
from qtpy.QtWidgets import (
    QApplication,
    QLabel,
    QTextEdit,
    QWidget,
)
from qtpynodeeditor import (
    NodeDataModel,
    NodeDataType,
    Port,
    PortCount,
)
from qtpynodeeditor.type_converter import TypeConverter

from .base_types import AnyData, register_types
from .custom_flow_view import CustomFlowView
from .image_models import ALL_MODELS as IMAGE_MODELS
from .lists_and_generators import (
    ALL_GENERATORS,
    ALL_LIST_MODELS,
    get_text_bounds,
)
from .logic import ALL_MODELS as LOGIC_MODELS
from .numbers import ALL_MODELS as NUMBER_MODELS
from .paths import ALL_MODELS as PATH_MODELS
from .signals import ALL_MODELS as SIGNAL_MODELS
from .signals import SignalHandler


class PrinterNode(NodeDataModel):
    num_ports = PortCount(1, 0)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._label = QLabel()

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return

        txt = pformat(node_data.item)
        self._label.setText(txt)
        self._label.setFixedSize(get_text_bounds(txt, self._label.font()))

    def embedded_widget(self) -> QWidget:
        return self._label


class DebugPrinterNode(NodeDataModel):
    name = "Debug print"
    num_ports = PortCount(1, 0)
    all_data_types = AnyData.data_type

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is not None:
            print(node_data.item)


class NoteNode(NodeDataModel):
    num_ports = PortCount(0, 0)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = QTextEdit(parent)

    def save(self) -> dict:
        dct = super().save()
        dct["text"] = self._widget.toPlainText()
        return dct

    def restore(self, doc: dict):
        if "text" in doc:
            self._widget.setPlainText(doc["text"])

    def embedded_widget(self) -> QWidget:
        return self._widget

    def resizable(self) -> bool:
        return True


def register_type(registry: ne.DataModelRegistry, from_: NodeDataType, to_: NodeDataType, converter: Callable):
    registry.register_type_converter(from_, to_, TypeConverter(from_, to_, converter))


def main(app):
    registry = ne.DataModelRegistry()

    model_dct = {
        "generators": ALL_GENERATORS,
        "lists": ALL_LIST_MODELS,
        "images": IMAGE_MODELS,
        "numbers": NUMBER_MODELS,
        "misc": [
            PrinterNode,
            DebugPrinterNode,
            NoteNode,
        ],
        "logic": LOGIC_MODELS,
        "paths": PATH_MODELS,
    }
    for category, models in model_dct.items():
        for model in models:
            registry.register_model(model, category)

    signal_handler = SignalHandler()
    for model in SIGNAL_MODELS:
        registry.register_model(model, "signals", signal_handler=signal_handler)

    register_types(registry)

    scene = ne.FlowScene(registry=registry)

    connection_style = scene.style_collection.connection

    # Configure the style collection to use colors based on data types:
    connection_style.use_data_defined_colors = True
    view = CustomFlowView(scene)
    view.setWindowTitle("Generator ui")
    view.resize(1920, 600)
    view.show()
    signal_handler.start()
    scene.load("text.flow")
    return scene, view, signal_handler


def app_main():
    # logging.basicConfig(level="DEBUG")
    app = QApplication([])
    scene, view, sh = main(app)
    scene.load("text.flow")
    app.exec_()
    scene.save("text.flow")
    sh.signal_queue.put(None)


if __name__ == "__main__":
    app_main()
