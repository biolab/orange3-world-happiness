from typing import Any, List, Set
from functools import partial
from re import match

from AnyQt.QtCore import Qt, Signal, QSortFilterProxyModel, QItemSelection, QItemSelectionModel, \
    QTimer, QModelIndex, QMimeData
from AnyQt.QtWidgets import QLabel, QGridLayout, QFormLayout, QLineEdit, \
    QTableView, QListView, QTreeWidget, QTreeWidgetItem, QAbstractItemView, QCheckBox, QSplitter, QVBoxLayout, \
    QSizePolicy
from AnyQt.QtGui import QStandardItemModel, QDrag

from Orange.data import Table
from Orange.data.pandas_compat import table_from_frame
from Orange.widgets.settings import Setting, ContextSetting
from Orange.widgets.utils import vartype
from Orange.widgets.utils.concurrent import ConcurrentWidgetMixin, TaskState
from Orange.widgets.utils.itemmodels import PyTableModel, PyListModel
from Orange.widgets.utils.listfilter import (
    VariablesListItemView, slices, variables_filter, delslice
)
from Orange.widgets.widget import OWWidget, Output
from Orange.widgets import gui
from PyQt5.QtCore import QAbstractItemModel

from orangecontrib.owwhstudy.whstudy import *

MONGO_HANDLE = WorldIndicators('main', 'biolab')


def source_model(view):
    """ Return the source model for the Qt Item View if it uses
    the QSortFilterProxyModel.
    """
    if isinstance(view.model(), QSortFilterProxyModel):
        return view.model().sourceModel()
    else:
        return view.model()


def source_indexes(indexes, view):
    """ Map model indexes through a views QSortFilterProxyModel
    """
    model = view.model()
    if isinstance(model, QSortFilterProxyModel):
        return list(map(model.mapToSource, indexes))
    else:
        return indexes


def run(
        countries: List,
        indicators: List,
        years: List,
        agg_method: int,
        index_freq: int,
        country_freq: int,
        state: TaskState
) -> Table:
    if not countries or not indicators or not years:
        return None

    # Define progress callback
    def callback(i: float, status=""):
        state.set_progress_value(i * 100)
        if status:
            state.set_status(status)
        if state.is_interruption_requested():
            raise Exception

    indicator_codes = [code for (_, code, desc, *other) in indicators]

    main_df = MONGO_HANDLE.data(countries, indicator_codes, years,
                                callback=callback, index_freq=index_freq, country_freq=country_freq)
    results = table_from_frame(main_df)

    results = AggregationMethods.aggregate(results, years, agg_method if len(years) > 1 else 0)

    exp_names = ['Topic', 'General Subject', 'Specific subject', 'Extension', 'Extension', 'Extension']
    # Add descriptions to indicators
    for attrib in results.domain.attributes:
        for (db, code, desc, ind_exp, is_rel, url) in indicators:
            if code in attrib.name:
                split = code.split(".")
                attrib.attributes["Description"] = desc
                for i in range(len(split)):
                    attrib.attributes[exp_names[i]] = f"{split[i]} - {ind_exp[i]}"

    return results


class CountryTreeWidgetItem(QTreeWidgetItem):
    def __init__(self, parent, key, code, duplicates: list):
        super().__init__(parent, key)
        self.country_code = code
        self.duplicates = []

    def setDuplicatesCheckState(self, p_int, Qt_CheckState):
        for dupl in self.duplicates:
            dupl.setCheckState(p_int, Qt_CheckState)


class CountryTreeWidgetItemWrapper(QTreeWidgetItem):
    def __init__(self, parent, key, code, item: CountryTreeWidgetItem):
        super().__init__(parent, [key])
        self.item = item
        self.country_code = code


class IndicatorTableView(QTableView):
    """ A Simple QTableView subclass initialized for displaying
    indicators.
    """
    dragDropActionDidComplete = Signal(int)

    def __init__(self, parent=None):
        super().__init__()

        self.setParent(parent)
        self.setSortingEnabled(True)
        self.setSelectionBehavior(QTableView.SelectRows)

        self.setSelectionMode(self.ExtendedSelection)
        self.setAcceptDrops(True)
        self.setDragEnabled(True)
        self.setDropIndicatorShown(True)
        self.setDragDropMode(self.DragDrop)
        self.setDefaultDropAction(Qt.MoveAction)
        self.setDragDropOverwriteMode(False)
        self.viewport().setAcceptDrops(True)

        self.setItemDelegate(gui.ColoredBarItemDelegate(self))
        self.horizontalHeader().setStretchLastSection(True)
        self.verticalHeader().setDefaultSectionSize(22)
        self.verticalHeader().hide()

    def startDrag(self, supported_actions):
        indices = self.selectionModel().selectedRows()
        if indices:
            data = self.model().mimeData(indices)
            if not data:
                return

            drag = QDrag(self)
            drag.setMimeData(data)

            default_action = Qt.IgnoreAction
            if self.defaultDropAction() != Qt.IgnoreAction and \
                    supported_actions & self.defaultDropAction():
                default_action = self.defaultDropAction()
            elif (supported_actions & Qt.CopyAction and
                  self.dragDropMode() != self.InternalMove):
                default_action = Qt.CopyAction
            res = drag.exec(supported_actions, default_action)
            if res == Qt.MoveAction:
                selected = self.selectionModel().selectedIndexes()
                rows = list(map(QModelIndex.row, selected))
                for s1, s2 in reversed(list(slices(rows))):
                    delslice(self.model(), s1, s2)
            self.dragDropActionDidComplete.emit(res)

    def dropEvent(self, event):
        # Bypass QListView.dropEvent on Qt >= 5.15.2.
        # Because `startDrag` is overridden and does not dispatch to base
        # implementation then `dropEvent` would need to be overridden also
        # (private `d->dropEventMoved` state tracking due to QTBUG-87057 fix).
        QAbstractItemView.dropEvent(self, event)

    def dragEnterEvent(self, event):
        """
        Reimplemented from QListView.dragEnterEvent
        """
        if self.acceptsDropEvent(event):
            event.accept()
        else:
            event.ignore()

    def acceptsDropEvent(self, event):
        """
        Should the drop event be accepted?
        """
        # disallow drag/drops between windows
        if event.source() is not None and \
                event.source().window() is not self.window():
            return False

        mime = event.mimeData()
        vars = mime.property('_items')
        if vars is None:
            return False

        event.accept()
        return True


class IndicatorTableModel(PyTableModel):
    """
    A Indicator table item model specialized for Drag and Drop.
    """
    MIME_TYPE = "application/x-Orange-IndicatorTableItemModelData"

    def __init__(self, *args, placeholder=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.placeholder = placeholder

    def wrap(self, table):
        super().wrap(table)

    def data(self, index, role=Qt.DisplayRole):
        return super().data(index, role)

    def flags(self, index):
        flags = super().flags(index)
        if index.isValid():
            flags |= Qt.ItemIsDragEnabled
        else:
            flags |= Qt.ItemIsDropEnabled
        return flags

    @staticmethod
    def supportedDropActions():
        return Qt.MoveAction  # pragma: no cover

    @staticmethod
    def supportedDragActions():
        return Qt.MoveAction  # pragma: no cover

    def mimeTypes(self):
        return [self.MIME_TYPE]

    def mimeData(self, indexlist):
        """
        Reimplemented.

        For efficiency reasons only the variable instances are set on the
        mime data (under `'_items'` property)
        """
        items = [self[index.row()] for index in indexlist]
        mime = QMimeData()
        mime.setData(self.MIME_TYPE, b'')
        mime.setProperty("_items", items)
        return mime

    def dropMimeData(self, mime, action, row, column, parent):
        """
        Reimplemented.
        """
        if action == Qt.IgnoreAction:
            return True  # pragma: no cover
        if not mime.hasFormat(self.MIME_TYPE):
            return False  # pragma: no cover
        indicators = mime.property("_items")
        if indicators is None:
            return False  # pragma: no cover
        if row < 0:
            row = self.rowCount()

        self[row:row] = indicators
        return True


class IndicatorFilterProxyModel(QSortFilterProxyModel):
    def __init__(self):
        super(IndicatorFilterProxyModel, self).__init__()
        self.rel_only = False
        self.and_filter = False
        self._filter_string = ""

    def set_filter_string(self, filter):
        self._filter_string = str(filter).lower()
        self.invalidateFilter()

    def set_rel(self, x):
        self.rel_only = x
        self.invalidateFilter()

    def set_and_filter(self):
        self.and_filter = not self.and_filter
        self.invalidateFilter()

    def filter_accepts_row(self, row):
        row_str = f"{row[0]} {row[1]} {row[3]}"
        row_str = row_str.lower()
        filters = self._filter_string.split()

        if self.and_filter:
            return all(f in row_str for f in filters)
        else:
            for f in filters:
                if f in row_str:
                    return True
        return not filters

    def filterAcceptsRow(self, source_row, source_parent):
        row = self.sourceModel()[source_row]
        return self.filter_accepts_row(row) and (not self.rel_only or (self.rel_only and row[4]))

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder):
        super().sort(column, order)


class OWWHStudy(OWWidget, ConcurrentWidgetMixin):
    name = "Socioeconomic Indices"
    description = "Gets requested socioeconomic data from WDB/WHR."
    icon = "icons/mywidget.svg"

    agg_method: int = Setting(AggregationMethods.NONE)
    indicator_freq: float = Setting(60)
    country_freq: float = Setting(90)
    selected_years: List = Setting([])
    selected_indicators: List = Setting([])
    selected_countries: Set = Setting(set({}))
    auto_apply: bool = Setting(False)
    splitter_state: bytes = Setting(b'')

    class Outputs:
        world_data = Output("World data", Table)

    def __init__(self):
        OWWidget.__init__(self)
        ConcurrentWidgetMixin.__init__(self)
        super().__init__()

        # Schedule interface updates (enabled buttons) using a coalescing
        # single shot timer (complex interactions on selection and filtering
        # updates in the 'available_attrs_view')
        self.__interface_update_timer = QTimer(self, interval=0, singleShot=True)
        self.__interface_update_timer.timeout.connect(
            self.__update_interface_state)
        # The last view that has the selection for move operation's source
        self.__last_active_view = None  # type: Optional[QListView]

        self.world_data = None
        self.year_features = []
        self.country_features = MONGO_HANDLE.countries()
        self.indicator_features = MONGO_HANDLE.indicators()

        self._setup_gui()

        # Assign values to control views
        self.year_features = [str(y) for y in range(2020, 1960, -1)]

        self.set_country_tree(self.country_features)

        self.available_indices_model.setHorizontalHeaderLabels(['Source', 'Index', 'Description'])
        self.selected_indices_model.setHorizontalHeaderLabels(['Source', 'Index', 'Description'])

        self.initial_indices_update()
        self.update_interface_state(self.available_indices_view)
        self.update_interface_state(self.selected_indices_view)

        self.resize(600, 600)

    def _setup_gui(self):
        fbox = gui.widgetBox(self.controlArea, "", orientation=0)
        fbox.setFixedWidth(550)
        controls_box = gui.widgetBox(fbox, "")
        tbox = gui.widgetBox(controls_box, "Indicator filtering", orientation=0)
        vbox = gui.hBox(tbox)
        sbox = gui.hBox(tbox)

        grid = QVBoxLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        vbox.layout().addLayout(grid)
        spin_box = gui.vBox(vbox, "Indicator frequency (%)")
        grid.addWidget(spin_box, alignment=Qt.AlignLeft)
        spin_box.setFixedWidth(grid.sizeHint().width())
        gui.spin(spin_box, self, 'indicator_freq', minv=1, maxv=100,
                 callback=self.__on_dummy_change,
                 tooltip="Percentage of received values to keep indicator."
                 )
        cspin_box = gui.vBox(vbox, "Country frequency (%)")
        grid.addWidget(cspin_box, alignment=Qt.AlignLeft)
        cspin_box.setFixedWidth(grid.sizeHint().width())
        gui.spin(cspin_box, self, 'country_freq', minv=1, maxv=100,
                 callback=self.__on_dummy_change,
                 tooltip="Percentage of received values to keep country.")

        agg_box = gui.vBox(vbox, "Aggreagtion by year")
        grid.addWidget(agg_box, alignment=Qt.AlignLeft)
        agg_box.setFixedWidth(grid.sizeHint().width())
        gui.comboBox(agg_box, self, 'agg_method', items=AggregationMethods.ITEMS,
                     callback=self.__on_dummy_change)

        gui.listBox(
            sbox, self, 'selected_years', labels='year_features',
            selectionMode=QListView.ExtendedSelection, box='Years',
            callback=self.__on_dummy_change
        )
        grid.addStretch()

        box = gui.widgetBox(controls_box, "Countries")

        self.country_tree = QTreeWidget()
        self.country_tree.setColumnCount(1)
        self.country_tree.setHeaderLabels(['Countries'])
        box.layout().addWidget(self.country_tree)
        self.country_tree.itemChanged.connect(self.country_checked)

        bbox = gui.vBox(controls_box)
        gui.auto_send(bbox, self, "auto_apply")

        def update_on_change(view):
            # Schedule interface state update on selection change in `view`
            self.__last_active_view = view
            self.__interface_update_timer.start()

        splitter = QSplitter(orientation=Qt.Vertical)
        abox = gui.widgetBox(splitter, "Available Indicators")
        sbox = gui.widgetBox(splitter, "Selected Indicators")

        self.__indicator_filter_line_edit = QLineEdit(placeholderText="Filter ...")

        self.__indicator_relative_checkbox = QCheckBox("Relative Only", self)
        self.__indicator_relative_checkbox.setToolTip("Toggle filtering only relative indicators.")
        self.__indicator_relative_checkbox.setChecked(False)
        self.__indicator_relative_checkbox.stateChanged.connect(
            self.__on_indicator_relative_changed
        )

        self.__indicator_and_button = gui.button(box, self, "Any", width=50, callback=self.__on_indicator_filter_changed)
        self.__indicator_and_button.adjustSize()
        self.__indicator_and_button.setToolTip("Toggle filtering indicators that include all words.")

        hBox = gui.hBox(abox)
        hBox.layout().addWidget(self.__indicator_filter_line_edit)
        hBox.layout().addWidget(self.__indicator_and_button)
        hBox.layout().addWidget(self.__indicator_relative_checkbox)
        abox.layout().addWidget(hBox)

        def dropcompleted(action):
            if action == Qt.MoveAction:
                self.commit()

        self.available_indices_model = IndicatorTableModel()
        self.available_indices_view = IndicatorTableView()
        proxy = IndicatorFilterProxyModel()
        proxy.setFilterKeyColumn(-1)
        proxy.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.__indicator_filter_line_edit.textChanged.connect(proxy.set_filter_string)
        self.available_indices_view.setModel(proxy)
        self.available_indices_view.model().setSourceModel(self.available_indices_model)
        self.available_indices_view.selectionModel().selectionChanged.connect(
            partial(update_on_change, self.available_indices_view))
        self.available_indices_view.dragDropActionDidComplete.connect(dropcompleted)
        abox.layout().addWidget(self.available_indices_view)

        self.selected_indices_model = IndicatorTableModel()
        self.selected_indices_view = IndicatorTableView()
        self.selected_indices_view.setModel(self.selected_indices_model)
        self.selected_indices_model.rowsInserted.connect(self.__selected_indicators_changed)
        self.selected_indices_model.rowsRemoved.connect(self.__selected_indicators_changed)
        self.selected_indices_view.selectionModel().selectionChanged.connect(
            partial(update_on_change, self.selected_indices_view))
        self.selected_indices_view.dragDropActionDidComplete.connect(dropcompleted)

        sbox.layout().addWidget(self.selected_indices_view)

        splitter.setSizes([300, 200])
        splitter.splitterMoved.connect(
            lambda:
            setattr(self, "splitter_state", bytes(splitter.saveState()))
        )
        self.mainArea.layout().addWidget(splitter)
        self.resize(1000, 500)

    def __update_interface_state(self):
        last_view = self.__last_active_view
        if last_view is not None:
            self.update_interface_state(last_view)

    def update_interface_state(self, focus=None):
        for view in [self.available_indices_view, self.selected_indices_view]:
            if view is not focus and not view.hasFocus() \
                    and view.selectionModel().hasSelection():
                view.selectionModel().clear()

        def selected_indices(view):
            model = source_model(view)
            return [model[i] for i in self.selected_rows(view)]

        available_selected = selected_indices(self.available_indices_view)
        indices_selected = selected_indices(self.selected_indices_view)

        move_indices_enabled = \
            (available_selected or indices_selected) and \
            self.selected_indices_view.isEnabled()

        self.available_indices_view.resizeColumnToContents(0)
        self.available_indices_view.resizeColumnToContents(1)
        self.available_indices_view.resizeColumnToContents(2)

        self.selected_indices_view.resizeColumnToContents(0)
        self.selected_indices_view.resizeColumnToContents(1)
        self.selected_indices_view.resizeColumnToContents(2)

        # Hide all collumns used in hover and etc.
        for i in range(3, self.available_indices_view.colorCount()):
            self.available_indices_view.setColumnHidden(i, True)
            self.selected_indices_view.setColumnHidden(i, True)

        self.__last_active_view = None
        self.__interface_update_timer.stop()

    def initial_indices_update(self):
        used = self.selected_indicators
        self.available_indices_model[:] = [index for index in self.indicator_features
                                           if index not in used]
        self.selected_indices_model[:] = self.selected_indicators
        self.commit()

    def __on_indicator_filter_changed(self):
        model = self.available_indices_view.model()
        model.set_and_filter()
        self.__indicator_and_button.setText("All" if model.and_filter else "Any")
        self._select_indicator_rows()

    def __on_indicator_relative_changed(self):
        model = self.available_indices_view.model()
        model.set_rel(self.__indicator_relative_checkbox.isChecked())
        self._select_indicator_rows()

    def __selected_indicators_changed(self):
        self.commit()

    def __on_indicator_horizontal_header_clicked(self):
        pass

    def __on_dummy_change(self):
        self.commit()

    def on_exception(self, ex: Exception):
        raise ex

    def on_done(self, result: Any):
        self.Outputs.world_data.send(result)

    def on_partial_result(self, result: Any) -> None:
        pass

    def commit(self):
        years = []
        for i in self.selected_years:
            years.append(int(self.year_features[i]))
        self.selected_indicators = self.selected_indices_model.tolist()
        self.start(
            run, list(self.selected_countries), self.selected_indicators,
            years, self.agg_method, self.indicator_freq, self.country_freq
        )

    def country_checked(self, item, column):
        if type(item) is CountryTreeWidgetItemWrapper:
            item.item.setDuplicatesCheckState(0, item.checkState(0))
            if item.checkState(column) == Qt.Checked:
                self.selected_countries.add(item.country_code)
            else:
                self.selected_countries.discard(item.country_code)
            self.commit()

    def _clear(self):
        self.clear_messages()
        self.cancel()
        self.selected_countries = set()
        self.selected_indicators = []
        self.selected_years = []

    def set_country_tree(self, data):
        root_node = QTreeWidgetItem(self.country_tree, ['All'])
        root_node.setExpanded(True)
        root_node.setFlags(root_node.flags() | Qt.ItemIsAutoTristate)

        geo_regions_node = QTreeWidgetItem(root_node, ['Geographical Regions'])
        geo_regions_node.setExpanded(True)
        geo_regions_node.setFlags(root_node.flags() | Qt.ItemIsAutoTristate)

        orgs_node = QTreeWidgetItem(root_node, ['Organisations'])
        orgs_node.setExpanded(True)
        orgs_node.setFlags(root_node.flags() | Qt.ItemIsAutoTristate)

        geo_region_list = []
        for (code, name, members) in GEO_REGIONS:
            geo_region_node = QTreeWidgetItem(geo_regions_node, [name])
            geo_region_node.setFlags(root_node.flags() | Qt.ItemIsAutoTristate)
            geo_region_list.append(geo_region_node)

        org_list = []
        for (code, name, members) in ORGANIZATIONS:
            org_node = QTreeWidgetItem(orgs_node, [name])
            org_node.setFlags(root_node.flags() | Qt.ItemIsAutoTristate)
            org_list.append(org_node)

        for (code, name) in sorted(data, key=lambda tup: tup[1]):
            item = CountryTreeWidgetItem(None, [name], code, [])
            state = Qt.Checked if code in self.selected_countries else Qt.Unchecked
            item.setCheckState(0, state)

            for i in range(len(GEO_REGIONS)):
                geo_node = geo_region_list[i]
                members = GEO_REGIONS[i][2]
                if code in members:
                    wrapper = CountryTreeWidgetItemWrapper(None, name, code, item)
                    item.duplicates.append(wrapper)
                    wrapper.setCheckState(0, state)
                    geo_node.addChild(wrapper)

            for i in range(len(ORGANIZATIONS)):
                org_node = org_list[i]
                members = ORGANIZATIONS[i][2]
                if code in members:
                    wrapper = CountryTreeWidgetItemWrapper(None, name, code, item)
                    item.duplicates.append(wrapper)
                    wrapper.setCheckState(0, state)
                    org_node.addChild(wrapper)

    def _select_indicator_rows(self):
        model = source_model(self.available_indices_view)
        available_selected = [model[r] for r in self.selected_rows(self.available_indices_view)]
        n_rows, n_columns = model.rowCount(), model.columnCount()
        selection = QItemSelection()
        for i in range(n_rows):
            indicator = model.data(model.index(i, 1))
            if indicator in available_selected:
                _selection = QItemSelection(model.index(i, 0),
                                            model.index(i, n_columns - 1))
                selection.merge(_selection, QItemSelectionModel.Select)

        self.available_indices_view.selectionModel().select(
            selection, QItemSelectionModel.ClearAndSelect
        )

    @staticmethod
    def selected_rows(view):
        """ Return the selected rows in the view.
        """
        rows = view.selectionModel().selectedRows()
        model = view.model()
        if isinstance(model, QSortFilterProxyModel):
            rows = [model.mapToSource(r) for r in rows]
        return [r.row() for r in rows]


if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview  # since Orange 3.20.0

    WidgetPreview(OWWHStudy).run()
