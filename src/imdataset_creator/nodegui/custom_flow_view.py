import difflib
import logging

import qtpynodeeditor as ne
from qtpy.QtCore import QLineF, QPoint, QRectF, Qt
from qtpy.QtGui import QContextMenuEvent, QKeyEvent, QKeySequence, QMouseEvent, QPainter, QPen, QShowEvent, QWheelEvent
from qtpy.QtWidgets import QLineEdit, QMenu, QTreeWidget, QTreeWidgetItem, QWidgetAction

logger = logging.getLogger(__name__)


class CustomFlowView(ne.FlowView):
    def generate_context_menu(self, pos: QPoint):
        """
        Generate a context menu for contextMenuEvent

        Parameters
        ----------
        pos : QPoint
            The point where the context menu was requested
        """
        model_menu = QMenu()
        skip_text = "skip me"

        # Add filterbox to the context menu
        txt_box = QLineEdit(model_menu)
        txt_box.setPlaceholderText("Filter")
        txt_box.setClearButtonEnabled(True)
        txt_box_action = QWidgetAction(model_menu)
        txt_box_action.setDefaultWidget(txt_box)
        model_menu.addAction(txt_box_action)

        # Add result treeview to the context menu
        tree_view = QTreeWidget(model_menu)
        tree_view.header().close()
        tree_view.setMinimumHeight(500)
        tree_view_action = QWidgetAction(model_menu)
        tree_view_action.setDefaultWidget(tree_view)
        model_menu.addAction(tree_view_action)

        top_level_items: dict[str, QTreeWidgetItem] = {}
        assert self._scene is not None
        for cat in self._scene.registry.categories():
            item = QTreeWidgetItem(tree_view)
            item.setText(0, cat)
            item.setData(0, Qt.ItemDataRole.UserRole, skip_text)
            top_level_items[cat] = item

        registry = self._scene.registry
        for model, category in registry.registered_models_category_association().items():
            self._parent = top_level_items[category]
            item = QTreeWidgetItem(self._parent)
            item.setText(0, model)
            item.setData(0, Qt.ItemDataRole.UserRole, model)

        tree_view.expandAll()

        def click_handler(item):
            assert self._scene is not None
            model_name = item.data(0, Qt.ItemDataRole.UserRole)
            if model_name == skip_text:
                return

            try:
                model, _ = self._scene.registry.get_model_by_name(model_name)
            except ValueError:
                logger.error("Model not found: %s", model_name)
            else:
                node = self._scene.create_node(model)
                pos_view = self.mapToScene(pos)
                assert node.graphics_object is not None
                node.graphics_object.setPos(pos_view)
                self._scene.node_placed.emit(node)

            model_menu.close()

        tree_view.itemClicked.connect(click_handler)

        # Setup filtering
        filter_handler = self._get_filter_handler(top_level_items)

        txt_box.textChanged.connect(filter_handler)

        # make sure the text box gets focus so the user doesn't have to click on it
        txt_box.setFocus()
        return model_menu

    def _get_filter_handler(self, top_level_items: dict[str, QTreeWidgetItem]):
        def filter_handler(text):
            children = {
                (child := top_lvl_item.child(i)).data(0, Qt.ItemDataRole.UserRole): child
                for top_lvl_item in top_level_items.values()
                for i in range(top_lvl_item.childCount())
            }

            close_matches = set(difflib.get_close_matches(text, children.keys(), n=10, cutoff=0.4))

            for top_lvl_item in top_level_items.values():
                num_hidden = 0
                num_children = top_lvl_item.childCount()
                for i in range(num_children):
                    child = top_lvl_item.child(i)
                    should_hide = child.data(0, Qt.ItemDataRole.UserRole) not in close_matches
                    child.setHidden(should_hide)
                    num_hidden += int(should_hide)
                top_lvl_item.setHidden(num_hidden == num_children)

        return filter_handler
