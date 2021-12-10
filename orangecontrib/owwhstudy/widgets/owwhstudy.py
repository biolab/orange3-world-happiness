from types import SimpleNamespace
from typing import Any

from AnyQt.QtWidgets import QLabel, QGridLayout

from Orange.data import Table
from Orange.widgets.utils.concurrent import ConcurrentWidgetMixin, TaskState
from Orange.widgets.widget import OWWidget
from Orange.widgets import gui


class Results(SimpleNamespace):
    data : Table


def run(
        state: TaskState
) -> Results:
    return Results()


class OWWHStudy(OWWidget, ConcurrentWidgetMixin):
    # Widget needs a name, or it is considered an abstract widget
    # and not shown in the menu.
    name = "World Happiness Study"
    description = "Gets requested world indicator data from WDB/WHR."
    icon = "icons/mywidget.svg"
    want_main_area = False

    def __init__(self):
        OWWidget.__init__(self)
        ConcurrentWidgetMixin.__init__(self)
        super().__init__()
        self._setup_gui()

    def _setup_gui(self):
        grid = QGridLayout()
        box = gui.widgetBox(self.controlArea, "Test", grid)

    def on_exception(self, ex: Exception):
        raise ex

    def on_done(self, result: Any) -> None:
        pass

    def on_partial_result(self, result: Any) -> None:
        pass


if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview  # since Orange 3.20.0
    WidgetPreview(OWWHStudy).run()
