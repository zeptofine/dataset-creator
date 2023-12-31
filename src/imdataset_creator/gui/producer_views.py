from __future__ import annotations

from ..datarules import base_rules, data_rules, image_rules
from .config_inputs import (
    ItemDeclaration,
    ItemSettings,
    ProceduralConfigList,
)
from .settings_inputs import DropdownInput


def producer_item(*args, **kwargs):
    return ItemDeclaration(*args, **kwargs, duplicable=False)


# class ProducerView(FlowItem):
#     title = "Producer"
#     movable = False

#     bound_item: type[base_rules.Producer]

#     def setup_widget(self):
#         super().setup_widget()
#         if self.desc:
#             self.desc += "\n"
#         self.desc += f"Produces: {set(self.bound_item.produces)}"
#         self.description_widget.setText(self.desc)


FileInfoProducerView = producer_item("File Info Producer", data_rules.FileInfoProducer)
ImShapeProducerView = producer_item("Image shape", image_rules.ImShapeProducer)
HashProducerView = producer_item(
    "Hash Producer",
    image_rules.HashProducer,
    desc="gets a hash for the contents of an image",
    settings=ItemSettings(
        {"hash_type": DropdownInput(list(image_rules.HASHERS.__members__.values())).label("Hash type:")}
    ),
)


def producer_list(parent=None) -> ProceduralConfigList:
    return ProceduralConfigList(
        FileInfoProducerView,
        ImShapeProducerView,
        HashProducerView,
        parent=parent,
        unique=True,
    ).label("Producers")
