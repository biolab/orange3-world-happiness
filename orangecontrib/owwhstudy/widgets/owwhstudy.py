from types import SimpleNamespace
from typing import Any, List

from AnyQt.QtCore import Qt, Signal
from AnyQt.QtWidgets import QLabel, QGridLayout, QFormLayout, QLineEdit, QTableView

from Orange.data import Table
from Orange.data.pandas_compat import table_from_frame
from Orange.widgets.settings import Setting
from Orange.widgets.utils.concurrent import ConcurrentWidgetMixin, TaskState
from Orange.widgets.utils.itemmodels import PyTableModel, TableModel
from Orange.widgets.widget import OWWidget
from Orange.widgets import gui

from orangecontrib.owwhstudy.whstudy import WorldIndicators, AggregationMethods

MONGO_HANDLE = WorldIndicators('main', 'biolab')


class Results(SimpleNamespace):
    data: Table


def run(
        countries: List,
        indicators: List,
        years: List,
        state: TaskState
) -> Results:
    results = Results(data=None)
    if not countries or not indicators or not years:
        return results

    # Define progress callback
    def callback(i: float, status=""):
        state.set_progress_value(i * 100)
        if status:
            state.set_status(status)
        if state.is_interruption_requested():
            raise Exception

    callback(0, "Fetching data ...")
    steps = len(countries)
    i = 1
    for country_code in countries:
        df = MONGO_HANDLE.data(country_code, indicators, years)

        if not results.data:
            results.data = table_from_frame(df)
        else:
            results.data.concatenate(table_from_frame(df))

        callback(i / steps)
        i += 1

    return results


class IndexTableView(QTableView):
    pressedAny = Signal()

    def __init__(self):
        super().__init__(
            sortingEnabled=True,
            editTriggers=QTableView.NoEditTriggers,
            selectionBehavior=QTableView.SelectRows,
            selectionMode=QTableView.ExtendedSelection,
            cornerButtonEnabled=False
        )
        self.verticalHeader().setDefaultSectionSize(22)
        self.verticalHeader().hide()

    def mousePressEvent(self, event):
        super().mousePressEvent(event)
        self.pressedAny.emit()


class IndexTableModel(PyTableModel):
    def data(self, index, role=Qt.DisplayRole):
        if role in (gui.BarRatioRole, Qt.DisplayRole):
            return super().data(index, Qt.EditRole)
        if role == Qt.BackgroundColorRole and index.column() == 0:
            return TableModel.ColorForRole[TableModel.Meta]
        return super().data(index, role)


class OWWHStudy(OWWidget, ConcurrentWidgetMixin):
    name = "Socioeconomic Indices"
    description = "Gets requested socioeconomic data from WDB/WHR."
    icon = "icons/mywidget.svg"
    want_main_area = False

    agg_method: int = Setting(AggregationMethods.MEAN)
    index_freq: float = Setting(60)

    def __init__(self):
        OWWidget.__init__(self)
        ConcurrentWidgetMixin.__init__(self)
        self.countries: List = []
        self.indicators: List = []
        super().__init__()
        self._setup_gui()

    def _setup_gui(self):
        fbox = gui.widgetBox(self.controlArea, orientation=0)

        box = gui.widgetBox(fbox, "Index Filtering")
        hbox = gui.hBox(box)
        grid = QFormLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        hbox.layout().addLayout(grid)
        spin = gui.spin(hbox, self, 'index_freq', minv=1, maxv=100)
        grid.addRow("Index frequency (%)", spin)

        box = gui.widgetBox(fbox, "Countries")
        self.__country_filter_line_edit = QLineEdit(
            textChanged=self.__on_country_filter_changed,
            placeholderText="Filter..."
        )
        box.layout().addWidget(self.__country_filter_line_edit)


        # TODO: Tree view for continents and countries with checkboxes

        box = gui.widgetBox(fbox, "Index Selection")
        self.__index_filter_line_edit = QLineEdit(
            textChanged=self.__on_index_filter_changed,
            placeholderText="Filter..."
        )
        box.layout().addWidget(self.__index_filter_line_edit)


    def on_exception(self, ex: Exception):
        raise ex

    def on_done(self, result: Any) -> None:
        pass

    def on_partial_result(self, result: Any) -> None:
        pass

    def __on_country_filter_changed(self):
        pass

    def __on_index_filter_changed(self):
        pass




if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview  # since Orange 3.20.0
    WidgetPreview(OWWHStudy).run()
