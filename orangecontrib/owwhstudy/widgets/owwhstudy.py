from types import SimpleNamespace
from typing import Any, List

from AnyQt.QtCore import Qt, Signal
from AnyQt.QtWidgets import QLabel, QGridLayout, QFormLayout, QLineEdit, \
    QTableView, QTableWidget, QTableWidgetItem, QVBoxLayout, QListView, QScrollArea, QHeaderView

from Orange.data import Table
from Orange.data.pandas_compat import table_from_frame
from Orange.widgets.settings import Setting, ContextSetting
from Orange.widgets.utils.concurrent import ConcurrentWidgetMixin, TaskState
from Orange.widgets.utils.itemmodels import PyTableModel, TableModel
from Orange.widgets.widget import OWWidget, Output
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


class IndexTableView(QTableWidget):
    def __init__(self, data, *args):
        super().__init__(
            sortingEnabled=True,
            editTriggers=QTableWidget.NoEditTriggers,
            selectionBehavior=QTableWidget.SelectRows,
            selectionMode=QTableWidget.ExtendedSelection,
            cornerButtonEnabled=False,
        )

        self.verticalHeader().hide()
        self.setRowCount(len(data))
        self.setColumnCount(3)
        self.setAlternatingRowColors(True)
        self.data = data
        self.horizontalHeader().setStretchLastSection(True)

        self.setData()
        self.resizeColumnToContents(0)
        self.resizeColumnToContents(1)
        self.resizeRowsToContents()
        self.verticalHeader().setDefaultSectionSize(50)

    def setData(self):
        horHeaders = ['Source', 'Group', 'Index name']
        for n, (code, desc, src, _) in enumerate(self.data):
            newitem = QTableWidgetItem(src)
            self.setItem(n, 0, newitem)
            newitem = QTableWidgetItem(code)
            self.setItem(n, 1, newitem)
            newitem = QTableWidgetItem(desc)
            self.setItem(n, 2, newitem)
        self.setHorizontalHeaderLabels(horHeaders)


class OWWHStudy(OWWidget, ConcurrentWidgetMixin):
    name = "Socioeconomic Indices"
    description = "Gets requested socioeconomic data from WDB/WHR."
    icon = "icons/mywidget.svg"
    want_main_area = False

    agg_method: int = Setting(AggregationMethods.MEAN)
    index_freq: float = Setting(60)
    year_indices: List = ContextSetting([0], exclude_metas=False)

    class Outputs:
        world_data = Output("World data", Table)

    def __init__(self):
        OWWidget.__init__(self)
        ConcurrentWidgetMixin.__init__(self)
        super().__init__()
        self.countries = MONGO_HANDLE.countries()
        self.indicators = MONGO_HANDLE.indicators()
        self.year_features = ['2020', '2019']
        self.year_indices = [0]
        self._setup_gui()

    def _setup_gui(self):
        fbox = gui.widgetBox(self.controlArea, orientation=0)

        box = gui.widgetBox(fbox, "Index Filtering")
        vbox = gui.hBox(box)

        grid = QFormLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        vbox.layout().addLayout(grid)
        spin = gui.spin(vbox, self, 'index_freq', minv=1, maxv=100)
        grid.addRow("Index frequency (%)", spin)

        vbox = gui.vBox(box)
        gui.listBox(
            vbox, self, 'year_indices', labels='year_features',
            selectionMode=QListView.ExtendedSelection, box='Years'
        )

        # TODO: Listbox for years not displaying correctly.

        box = gui.vBox(box, "Aggregation by year")
        gui.comboBox(
            box, self, "agg_method", items=AggregationMethods.ITEMS,
            callback=self.set_aggregation
        )


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
        self.index_table = IndexTableView(self.indicators)
        box.layout().addWidget(self.__index_filter_line_edit)
        box.layout().addWidget(self.index_table)
        self.index_table.show()

    def on_exception(self, ex: Exception):
        raise ex

    def on_done(self, result: Any) -> None:
        pass

    def on_partial_result(self, result: Any) -> None:
        pass

    def set_aggregation(self):
        pass

    def __on_country_filter_changed(self):
        pass

    def __on_index_filter_changed(self):
        pass




if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview  # since Orange 3.20.0
    WidgetPreview(OWWHStudy).run()
