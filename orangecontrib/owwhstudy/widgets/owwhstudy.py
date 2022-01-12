from typing import Any, List, Set
from functools import partial

from AnyQt.QtCore import Qt, Signal, QSortFilterProxyModel, QItemSelection, QItemSelectionModel, \
    QTimer, QModelIndex, QMimeData
from AnyQt.QtWidgets import QLabel, QVBoxLayout, QFormLayout, QLineEdit, \
    QTableView, QListView, QTreeWidget, QTreeWidgetItem, QAbstractItemView, QCheckBox
from AnyQt.QtGui import QStandardItem, QDrag

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
from Orange.widgets.data.owselectcolumns import VariablesListItemModel
from Orange.widgets import gui
from PyQt5.QtCore import QAbstractItemModel

from orangecontrib.owwhstudy.whstudy import WorldIndicators, AggregationMethods

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

    indicator_codes = [code for (_, code, _, _) in indicators]

    main_df = MONGO_HANDLE.data(countries, indicator_codes, years, callback=callback, index_freq=index_freq)
    results = table_from_frame(main_df)

    results = AggregationMethods.aggregate(results, years, agg_method if len(years) > 1 else 0)

    # Add descriptions to indicators
    for attrib in results.domain.attributes:
        for (_, code, _, desc) in indicators:
            if code in attrib.name:
                attrib.attributes["description"] = desc

    return results


class CountryTreeWidgetItem(QTreeWidgetItem):
    def __init__(self, parent, key, code):
        super().__init__(parent, [key])
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


class IndicatorTableItem(QStandardItem):
    def __init__(self, text):
        super().__init__(text)


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

    def setRelOnly(self, x):
        self.rel_only = x
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        show = QSortFilterProxyModel.filterAcceptsRow(self, source_row, source_parent)
        if self.rel_only:
            return show and (self.sourceModel()[source_row][2])
        else:
            return show

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder):
        super().sort(column, order)


class OWWHStudy(OWWidget, ConcurrentWidgetMixin):
    name = "Socioeconomic Indices"
    description = "Gets requested socioeconomic data from WDB/WHR."
    icon = "icons/mywidget.svg"
    want_main_area = True
    resizing_enabled = True

    agg_method: int = Setting(AggregationMethods.NONE)
    indicator_freq: float = Setting(60)
    selected_years: List = Setting([])
    selected_indicators: List = Setting([])
    selected_countries: Set = Setting(set(), schema_only=True)
    auto_apply: bool = Setting(False)

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

        ctree = self.create_country_tree(self.country_features)
        self.country_tree.itemChanged.connect(self.country_checked)
        self.set_country_tree(ctree, self.country_tree)

        self.available_indices_model.setHorizontalHeaderLabels(['Source', 'Index', 'Relative', 'Description'])
        self.selected_indices_model.setHorizontalHeaderLabels(['Source', 'Index', 'Relative', 'Description'])

        self.initial_indices_update()
        self.update_interface_state(self.available_indices_view)
        self.update_interface_state(self.selected_indices_view)

        self.resize(600, 600)

    def _setup_gui(self):
        fbox = gui.widgetBox(self.controlArea, "", orientation=0)

        box = gui.widgetBox(fbox, "Indicator Filtering")
        vbox = gui.hBox(box)

        grid = QFormLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        vbox.layout().addLayout(grid)
        spin = gui.spin(vbox, self, 'indicator_freq', minv=1, maxv=100)
        grid.addRow("Index frequency (%)", spin)

        vbox = gui.vBox(box)
        years_view = gui.listBox(
            vbox, self, 'selected_years', labels='year_features',
            selectionMode=QListView.ExtendedSelection, box='Years'
        )

        abox = gui.vBox(box, "Aggregation by year")
        gui.comboBox(
            abox, self, "agg_method", items=AggregationMethods.ITEMS
        )
        bbox = gui.vBox(box)
        gui.auto_send(bbox, self, "auto_apply")

        box = gui.widgetBox(fbox, "Countries")

        self.country_tree = QTreeWidget()
        self.country_tree.setFixedWidth(400)
        self.country_tree.setColumnCount(1)
        self.country_tree.setColumnWidth(0, 300)
        self.country_tree.setHeaderLabels(['Countries'])
        box.layout().addWidget(self.country_tree)

        def update_on_change(view):
            # Schedule interface state update on selection change in `view`
            self.__last_active_view = view
            self.__interface_update_timer.start()

        abox = gui.widgetBox(self.mainArea, "Available Indicators")
        bbox = gui.widgetBox(self.mainArea, "")
        sbox = gui.widgetBox(self.mainArea, "Selected Indicators")

        self.__indicator_filter_line_edit = QLineEdit(
            textChanged=self.__on_indicator_filter_changed,
            placeholderText="Filter ..."
        )

        self.__indicator_relative_checkbox = QCheckBox("Relative Only", self,)
        self.__indicator_relative_checkbox.setChecked(False)
        self.__indicator_relative_checkbox.stateChanged.connect(
            self.__on_indicator_relative_changed
        )

        hBox = gui.hBox(abox)
        hBox.layout().addWidget(self.__indicator_filter_line_edit)
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

        # Create button for move
        self.move_indicators_button = gui.button(
            box, self, "∧",
            width=30,
            callback=partial(self.move_selected, self.selected_indices_view)
        )
        vLayout = QVBoxLayout()
        vLayout.setAlignment(Qt.AlignHCenter)
        vLayout.addWidget(self.move_indicators_button)
        bbox.layout().addLayout(vLayout)
        sbox.layout().addWidget(self.selected_indices_view)

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
        self.move_indicators_button.setEnabled(bool(move_indices_enabled))
        if move_indices_enabled:
            self.move_indicators_button.setText("∨" if available_selected else "∧")

        self.available_indices_view.resizeColumnToContents(0)
        self.available_indices_view.resizeColumnToContents(1)
        self.available_indices_view.resizeColumnToContents(2)

        self.selected_indices_view.resizeColumnToContents(0)
        self.selected_indices_view.resizeColumnToContents(1)
        self.selected_indices_view.resizeColumnToContents(2)

        self.available_indices_view.setColumnHidden(2, True)
        self.selected_indices_view.setColumnHidden(2, True)


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
        model.setFilterFixedString(self.__indicator_filter_line_edit.text().strip())
        self._select_indicator_rows()

    def __on_indicator_relative_changed(self):
        model = self.available_indices_view.model()
        model.setRelOnly(self.__indicator_relative_checkbox.isChecked())
        self._select_indicator_rows()

    def __selected_indicators_changed(self):
        self.commit()

    def __on_indicator_horizontal_header_clicked(self):
        pass

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
            years, self.agg_method, self.indicator_freq
        )

    def country_checked(self, item: CountryTreeWidgetItem, column):
        if item.country_code is not None:
            if item.checkState(column) == Qt.Checked:
                self.selected_countries.add(item.country_code)
            else:
                self.selected_countries.discard(item.country_code)

    def _clear(self):
        self.clear_messages()
        self.cancel()
        self.selected_countries = set()
        self.selected_indicators = []
        self.selected_years = []

    @staticmethod
    def create_country_tree(data):
        regions = [('AFR', 'Africa'), ('ECS', 'Europe & Central Asia'),
                   ('EAS', 'East Asia & Pacific'), ('LCN', 'Latin America and the Caribbean'),
                   ('NAC', 'North America'), ('SAS', 'South Asia')]
        members = [
            {'RWA', 'TZA', 'DZA', 'MAR', 'SEN', 'BDI', 'MOZ', 'GIN', 'EGY', 'MUS', 'GNQ', 'CIV', 'ZAF', 'SLE', 'STP',
             'UGA', 'ZWE', 'GNB', 'AGO', 'MDG', 'CPV', 'TCD', 'COD', 'COM', 'MWI', 'LSO', 'NGA', 'COG', 'NER', 'BFA',
             'SYC', 'SSD', 'TGO', 'ETH', 'TUN', 'SOM', 'KEN', 'DJI', 'BWA', 'LBY', 'ERI', 'GHA', 'GAB', 'GMB', 'CMR',
             'MRT', 'SDN', 'SWZ', 'BEN', 'NAM', 'MLI', 'LBR', 'CAF', 'ZMB'},
            {'AUT', 'HRV', 'SWE', 'TJK', 'AND', 'UKR', 'TUR', 'NOR', 'BIH', 'FIN', 'FRO', 'CYP', 'GBR', 'HUN', 'ISL',
             'BEL', 'PRT', 'MCO', 'IMN', 'LTU', 'MDA', 'SVK', 'CHI', 'TKM', 'LVA', 'SRB', 'MKD', 'FRA', 'LIE', 'ESP',
             'GRL', 'GIB', 'ITA', 'SMR', 'IRL', 'DNK', 'POL', 'AZE', 'CHE', 'EST', 'ARM', 'KAZ', 'LUX', 'ALB', 'GEO',
             'NLD', 'DEU', 'SVN', 'CZE', 'MNE', 'RUS', 'BLR', 'GRC', 'XKX', 'UZB', 'KGZ', 'ROU', 'BGR'},
            {'JPN', 'PRK', 'VNM', 'THA', 'FSM', 'HKG', 'MMR', 'SGP', 'TUV', 'TON', 'PNG', 'VUT', 'NRU', 'ASM', 'PYF',
             'MYS', 'SLB', 'AUS', 'FJI', 'BRN', 'MNG', 'PHL', 'PLW', 'TWN', 'KHM', 'KOR', 'KIR', 'MHL', 'LAO', 'GUM',
             'MNP', 'IDN', 'WSM', 'MAC', 'NCL', 'NZL', 'TLS', 'CHN'},
            {'GRD', 'BRB', 'SUR', 'VEN', 'DOM', 'BOL', 'GTM', 'LCA', 'JAM', 'VCT', 'HTI', 'PER', 'SXM', 'TCA', 'GUY',
             'MAF', 'ECU', 'BHS', 'MEX', 'ATG', 'HND', 'VIR', 'KNA', 'DMA', 'BLZ', 'PRI', 'NIC', 'COL', 'CYM', 'URY',
             'VGB', 'CHL', 'PAN', 'BRA', 'TTO', 'ABW', 'CUB', 'ARG', 'SLV', 'CUW', 'PRY', 'CRI'}, {'USA', 'CAN', 'BMU'},
            {'LKA', 'MDV', 'IND', 'AFG', 'NPL', 'BGD', 'BTN', 'PAK'}]
        tree = {'All': {
            'Regions': {
                'Africa': [],
                'Europe & Central Asia': [],
                'East Asia & Pacific': [],
                'Latin America and the Caribbean': [],
                'North America': [],
                'South Asia': []
            }
        }}
        regions_node = tree['All']['Regions']
        for (code, name) in data:
            for ind in range(len(regions)):
                if code in members[ind]:
                    regions_node[regions[ind][1]].append((code, name))
        return tree

    def set_country_tree(self, data, parent):
        for key in sorted(data):
            node = CountryTreeWidgetItem(parent, key[1], key[0]) if isinstance(key, tuple) else \
                CountryTreeWidgetItem(parent, key, None)
            state = Qt.Checked if key in self.selected_countries else Qt.Unchecked
            node.setCheckState(0, state)
            if isinstance(data, dict) and data[key]:
                node.setExpanded(key == 'All' or key == 'Regions')
                node.setFlags(node.flags() | Qt.ItemIsAutoTristate)
                self.set_country_tree(data[key], node)

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

    def move_rows(self, view: QListView, offset: int, roles=(Qt.EditRole,)):
        rows = [idx.row() for idx in view.selectionModel().selectedRows()]
        model = view.model()  # type: QAbstractItemModel
        rowcount = model.rowCount()
        newrows = [min(max(0, row + offset), rowcount - 1) for row in rows]

        def itemData(index):
            return {role: model.data(index, role) for role in roles}

        for row, newrow in sorted(zip(rows, newrows), reverse=offset > 0):
            d1 = itemData(model.index(row, 0))
            d2 = itemData(model.index(newrow, 0))
            model.setItemData(model.index(row, 0), d2)
            model.setItemData(model.index(newrow, 0), d1)

        selection = QItemSelection()
        for nrow in newrows:
            index = model.index(nrow, 0)
            selection.select(index, index)
        view.selectionModel().select(
            selection, QItemSelectionModel.ClearAndSelect)

        self.commit()

    def move_up(self, view: QListView):
        self.move_rows(view, -1)

    def move_down(self, view: QListView):
        self.move_rows(view, 1)

    def move_selected(self, view):
        if self.selected_rows(view):
            self.move_selected_from_to(view, self.available_indices_view)
        elif self.selected_rows(self.available_indices_view):
            self.move_selected_from_to(self.available_indices_view, view)

    def move_selected_from_to(self, src, dst):
        self.move_from_to(src, dst, self.selected_rows(src))

    def move_from_to(self, src, dst, rows):
        src_model = source_model(src)
        attrs = [src_model[r] for r in rows]

        for s1, s2 in reversed(list(slices(rows))):
            del src_model[s1:s2]

        dst_model = source_model(dst)

        if dst_model.rowCount() == 0:
            dst_model.wrap(attrs)
        else:
            dst_model.extend(attrs)

        self.commit()


if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview  # since Orange 3.20.0

    WidgetPreview(OWWHStudy).run()
