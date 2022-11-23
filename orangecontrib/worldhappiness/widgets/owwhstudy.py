from typing import Any, Set, Optional

from AnyQt.QtCore import Qt, Signal, QSortFilterProxyModel, QItemSelection, QItemSelectionModel, \
    QModelIndex, QMimeData
from AnyQt.QtWidgets import QLineEdit, \
    QTableView, QListView, QTreeWidget, QTreeWidgetItem, QAbstractItemView, QCheckBox, QSplitter, QVBoxLayout, \
    QApplication
from AnyQt.QtGui import QDrag, QClipboard

from Orange.widgets.settings import Setting
from Orange.widgets.utils.concurrent import ConcurrentWidgetMixin, TaskState
from Orange.widgets.utils.itemmodels import PyTableModel
from Orange.widgets.utils.listfilter import (
    slices, delslice
)
from Orange.widgets.utils.tableview import table_selection_to_mime_data
from Orange.widgets.widget import OWWidget, Output, Input
from Orange.widgets import gui

from orangecontrib.worldhappiness.whstudy import *

MONGO_HANDLE = WorldIndicators('main', 'biolab')
EXP_NAMES = ['Topic', 'General Subject', 'Specific subject', 'Extension', 'Extension', 'Extension']
DB_NAMES = [('WDI', 'World Data Indicators'),
            ('WHR', 'World Happiness Report'),
            ('HSL_OECD', 'How\'s life \r\nOrganization for Economic Co-operation and Development')]


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

    main_df = MONGO_HANDLE.data(countries, indicator_codes, years, callback=callback,
                                index_freq=index_freq, country_freq=country_freq)

    results = table_from_frame(main_df)
    results = AggregationMethods.aggregate(results, agg_method=agg_method if len(years) > 1 else 0,
                                           index_freq=index_freq, country_freq=country_freq, callback=callback)

    # Add descriptions to indicators
    if results:
        for attrib in results.domain.attributes:
            for (db, code, desc, ind_exp, is_rel, url, *_) in indicators:
                if code in attrib.name:
                    attrib.attributes["Description"] = desc
                    if len(ind_exp) > 0:
                        split = code.split(".")
                        for i in range(len(ind_exp)):
                            attrib.attributes[EXP_NAMES[min(i, len(EXP_NAMES)-1)]] = f"{split[i]} - {ind_exp[i]}"
    return results


class CountryTreeWidgetItem(QTreeWidgetItem):
    def __init__(self, parent, key, code):
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
    keyPressed = Signal(int)

    def __init__(self, parent=None):
        super().__init__()

        self.setParent(parent)
        self.setSelectionBehavior(QTableView.SelectRows)

        self.setSelectionMode(self.ExtendedSelection)
        self.setAcceptDrops(True)
        self.setDragEnabled(True)
        self.setSortingEnabled(True)
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
                selected = self.selectionModel().selectedRows()
                rows = list(map(QModelIndex.row, selected))
                model = source_model(self)
                rows = model.mapToSourceRows(rows)
                model.resetSorting()
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

    def keyPressEvent(self, event):
        super().keyPressEvent(event)
        self.keyPressed.emit(event.key())

class IndicatorTableModel(PyTableModel):
    """
    A Indicator table item model specialized for Drag and Drop.
    """
    MIME_TYPE = "application/x-Orange-IndicatorTableItemModelData"

    def __init__(self, *args, placeholder=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.placeholder = placeholder
        self.setHorizontalHeaderLabels(['Source', 'Indicator', 'Description', '', '', ''])

    def wrap(self, table):
        super().wrap(table)

    def setData(self, index, value, role):
        row = self.mapFromSourceRows(index.row())
        if role == Qt.EditRole:
            self[row][index.column()] = value
            self.dataChanged.emit(index, index)
        else:
            self._roleData[row][index.column()][role] = value
        return True

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.ToolTipRole:
            row = self.mapFromSourceRows(index.row())
            col = index.column()
            if col == 0:
                value = self[row][col]
                for (code, tooltip) in DB_NAMES:
                    if code == value:
                        return tooltip
            if col == 1:
                code = self[row][col]
                exp = self[row][3]
                split = code.split(".")
                tips = []
                if len(split) != len(exp):
                    print(split)
                    print(exp)
                for i in range(min(len(split), len(exp))):
                    tips.append(f"{split[i]} - {exp[i]}")
                return "\r\n".join(tips)
            return ""
        else:
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
        self.resetSorting()
        self[row:row] = indicators

        return True


class IndicatorFilterProxyModel(QSortFilterProxyModel):
    def __init__(self):
        super(IndicatorFilterProxyModel, self).__init__()
        self.rel_only = False
        self.rem_sparse = False
        self.and_filter = False
        self._filter_string = ""

    def set_filter_string(self, filter):
        self._filter_string = str(filter).lower()
        self.invalidateFilter()

    def set_rel(self, x):
        self.rel_only = x
        self.invalidateFilter()

    def set_sparse(self, x):
        self.rem_sparse = x
        self.invalidateFilter()

    def set_and_filter(self):
        self.and_filter = not self.and_filter
        self.invalidateFilter()

    def filter_accepts_row(self, row):
        if len(row) > 0:
            row_str = f"{row[0]} {row[1]} {row[2]}"
            row_str = row_str.lower()
            filters = self._filter_string.split()

            if self.and_filter:
                return all(f in row_str for f in filters)
            else:
                for f in filters:
                    if f in row_str:
                        return True
            return not filters
        return False

    def filterAcceptsRow(self, source_row, source_parent):
        row = self.sourceModel()[source_row]
        return self.filter_accepts_row(row) and \
               (not self.rel_only or (self.rel_only and row[4])) and \
               (not self.rem_sparse or (self.rem_sparse and not row[6]))

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder):
        super().sort(column, order)


class OWWHStudy(OWWidget, ConcurrentWidgetMixin):
    name = "Socioeconomic Indices"
    description = "Gets requested socioeconomic data from a remote database with various indicators."
    icon = "icons/socioeconomicindices.svg"

    agg_method: int = Setting(AggregationMethods.MEAN)
    indicator_freq: float = Setting(60)
    country_freq: float = Setting(90)
    selected_years: List = Setting([])
    selected_indicators: List = Setting([])
    selected_countries: Set = Setting(set({}))
    auto_apply: bool = Setting(False)
    splitter_state: bytes = Setting(b'')

    class Inputs:
        indicators = Input("Indicators", Table)

    class Outputs:
        world_data = Output("World data", Table)

    def __init__(self):
        OWWidget.__init__(self)
        ConcurrentWidgetMixin.__init__(self)
        super().__init__()

        self.world_data = None
        self.year_features = MONGO_HANDLE.years()

        self._setup_gui()

        # Assign values to control views
        self.year_features = MONGO_HANDLE.years()
        self.country_features = MONGO_HANDLE.countries()
        self.indicator_features = MONGO_HANDLE.indicators()

        self.set_country_tree(self.country_features)

        self.initial_indices_update()

        self.resize(1400, 800)

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
        spin_box.setFixedWidth(175)
        gui.spin(spin_box, self, 'indicator_freq', minv=1, maxv=100,
                 callback=self.__on_dummy_change,
                 tooltip="Percentage of received values to keep indicator."
                 )
        cspin_box = gui.vBox(vbox, "Country frequency (%)")
        grid.addWidget(cspin_box, alignment=Qt.AlignLeft)
        cspin_box.setFixedWidth(175)
        gui.spin(cspin_box, self, 'country_freq', minv=1, maxv=100,
                 callback=self.__on_dummy_change,
                 tooltip="Percentage of received values to keep country.")

        agg_box = gui.vBox(vbox, "Aggreagtion by year")
        grid.addWidget(agg_box, alignment=Qt.AlignLeft)
        agg_box.setFixedWidth(175)
        gui.comboBox(agg_box, self, 'agg_method', items=AggregationMethods.ITEMS,
                     callback=self.__on_dummy_change)

        self.years_list = gui.listBox(
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
        self.country_tree.itemClicked.connect(self.__on_dummy_change)

        bbox = gui.vBox(controls_box)
        gui.auto_send(bbox, self, "auto_apply")

        splitter = QSplitter(orientation=Qt.Vertical)
        self.available_box = gui.widgetBox(splitter, f"Available Indicators")
        self.selected_box = gui.widgetBox(splitter, f"Selected Indicators")

        self.__indicator_filter_line_edit = QLineEdit(placeholderText="Filter ...")

        self.__indicator_relative_checkbox = QCheckBox("Relative Only", self)
        self.__indicator_relative_checkbox.setToolTip("Toggle filtering only relative indicators.")
        self.__indicator_relative_checkbox.setChecked(False)
        self.__indicator_relative_checkbox.stateChanged.connect(
            self.__on_indicator_relative_changed
        )

        self.__indicator_sparse_checkbox = QCheckBox("Remove Sparse", self)
        self.__indicator_sparse_checkbox.setToolTip("Remove very sparse indicators.")
        self.__indicator_sparse_checkbox.setChecked(False)
        self.__indicator_sparse_checkbox.stateChanged.connect(
            self.__on_indicator_relative_changed
        )

        self.__indicator_and_button = gui.button(box, self, "Any", width=50, callback=self.__on_indicator_filter_changed)
        self.__indicator_and_button.adjustSize()
        self.__indicator_and_button.setToolTip("Toggle filtering indicators that include all words.")

        filters_row = gui.hBox(self.available_box)
        filters_row.layout().addWidget(self.__indicator_filter_line_edit)
        filters_row.layout().addWidget(self.__indicator_and_button)
        filters_row.layout().addWidget(self.__indicator_relative_checkbox)
        filters_row.layout().addWidget(self.__indicator_sparse_checkbox)
        self.available_box.layout().addWidget(filters_row)

        def dropcompleted(action):
            if action == Qt.MoveAction:
                self.fix_redraw()
                self.commit.deferred()

        self.available_indices_model = IndicatorTableModel()
        self.available_indices_view = IndicatorTableView()
        proxy = IndicatorFilterProxyModel()
        proxy.setFilterKeyColumn(-1)
        proxy.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.__indicator_filter_line_edit.textChanged.connect(proxy.set_filter_string)
        self.__indicator_filter_line_edit.textChanged.connect(self.fix_redraw)
        self.available_indices_view.setModel(proxy)
        self.available_indices_view.model().setSourceModel(self.available_indices_model)
        self.available_indices_view.dragDropActionDidComplete.connect(dropcompleted)
        self.available_indices_view.selectionModel().selectionChanged.connect(self.fix_redraw)
        self.available_box.layout().addWidget(self.available_indices_view)

        self.selected_indices_model = IndicatorTableModel()
        self.selected_indices_view = IndicatorTableView()
        self.selected_indices_view.setModel(self.selected_indices_model)
        self.selected_indices_view.selectionModel().selectionChanged.connect(self.fix_redraw)

        self.selected_indices_model.rowsInserted.connect(self.__on_dummy_change)
        self.selected_indices_model.rowsRemoved.connect(self.__on_dummy_change)
        self.selected_indices_view.dragDropActionDidComplete.connect(dropcompleted)
        self.selected_indices_view.keyPressed.connect(self.__on_indicator_delete)
        self.selected_box.layout().addWidget(self.selected_indices_view)

        splitter.setSizes([300, 200])
        splitter.splitterMoved.connect(
            lambda:
            setattr(self, "splitter_state", bytes(splitter.saveState()))
        )
        self.mainArea.layout().addWidget(splitter)

    def fix_redraw(self):
        self.available_indices_view.setColumnWidth(0, 47)
        self.available_indices_view.setColumnWidth(1, 142)
        self.selected_indices_view.setColumnWidth(0, 47)
        self.selected_indices_view.setColumnWidth(1, 142)

        # Hide all collumns used in hover and etc.
        for i in range(3, self.available_indices_view.model().columnCount()):
            self.available_indices_view.setColumnHidden(i, True)
        for i in range(3, self.selected_indices_view.model().columnCount()):
            self.selected_indices_view.setColumnHidden(i, True)

        self.available_box.setTitle(f'Available Indicators     '
                                    f'{self.available_indices_view.model().rowCount()} / '
                                    f'{self.available_indices_model.rowCount()} displayed | '
                                    f'{int(len(self.available_indices_view.selectedIndexes())/3)} chosen')
        self.selected_box.setTitle(f'Selected Indicators     '
                                   f'{self.selected_indices_view.model().rowCount()} displayed | '
                                   f'{int(len(self.selected_indices_view.selectedIndexes())/3)} chosen')

    def initial_indices_update(self):
        used = self.selected_indicators
        self.available_indices_model[:] = [index for index in self.indicator_features
                                           if index not in used]
        self.selected_indices_model[:] = self.selected_indicators

        self.selected_years = self.selected_years if len(self.selected_years) > 0 else list(range(10))
        self.fix_redraw()

        # If big querry make sure auto apply is False
        if len(used) > 50 or len(self.selected_countries) > 10 or len(self.selected_years) > 10:
            self.auto_apply = False

        self.commit.deferred()

    def __on_indicator_filter_changed(self):
        model = self.available_indices_view.model()
        model.set_and_filter()
        self.__indicator_and_button.setText("All" if model.and_filter else "Any")
        self._select_indicator_rows()

    def __on_indicator_relative_changed(self):
        model = self.available_indices_view.model()
        model.set_rel(self.__indicator_relative_checkbox.isChecked())
        model.set_sparse(self.__indicator_sparse_checkbox.isChecked())
        self._select_indicator_rows()

    def __on_indicator_delete(self, key):
        if key == Qt.Key_Delete or key == Qt.Key_Backspace:
            rows = self.selected_rows(self.selected_indices_view)
            if rows:
                src_model = source_model(self.selected_indices_view)
                unsorted_rows = src_model.mapToSourceRows(rows)
                indics = [src_model[r] for r in unsorted_rows]

                order = self.selected_indices_view.horizontalHeader().sortIndicatorOrder()
                col = self.selected_indices_view.horizontalHeader().sortIndicatorSection()

                src_model.resetSorting()

                for s1, s2 in reversed(list(slices(unsorted_rows))):
                    delslice(src_model, s1, s2)

                if col <= 2:
                    self.selected_indices_view.sortByColumn(col, order)

                dst_model = source_model(self.available_indices_view)
                dst_model.extend(indics)

                self.commit.deferred()

                self.fix_redraw()

    def __on_dummy_change(self):
        self.commit.deferred()

    def on_exception(self, ex: Exception):
        raise ex

    def on_done(self, result: Any):
        self.Outputs.world_data.send(result)

    def on_partial_result(self, result: Any) -> None:
        pass

    def copy_to_clipboard(self):
        self.copyRow()

    def copyRow(self):
        mime_available = table_selection_to_mime_data(self.available_indices_view)
        mime_selected = table_selection_to_mime_data(self.selected_indices_view)
        if mime_available.text():
            QApplication.clipboard().setMimeData(mime_available, QClipboard.Clipboard)
        elif mime_selected.text():
            QApplication.clipboard().setMimeData(mime_selected, QClipboard.Clipboard)

    @Inputs.indicators
    def set_inputs(self, inputs: Optional[Table]):
        if inputs is not None and inputs.domain is not None:
            input_indicators = []
            for col in inputs.domain:
                if isinstance(col, ContinuousVariable) and not re.match(r'\d+-.*', col.name):
                    indicator = [ind for ind in self.indicator_features if ind[1] == col.name]
                    if indicator:
                        input_indicators.extend(indicator)
            if 0 < len(input_indicators):
                self.selected_indicators = input_indicators
                self.initial_indices_update()

    @gui.deferred
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
            item = CountryTreeWidgetItem(None, [name], code)
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
