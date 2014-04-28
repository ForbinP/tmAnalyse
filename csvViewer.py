#! /usr/bin/env python

import wx
import sys
import argparse
import csv

import wx.lib.mixins.listctrl as listmix

g_version = 0.1

g_description = ('''
Create a wxWindows GUI with a ListCtrl for viewing CSV data.

v{}
'''.format(g_version))


def parse_command_args(args):

    parser = argparse.ArgumentParser(
        description=g_description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        'infilename', nargs='?',  # '?' => optional param
        help='CSV file to read (otherwise, use stdin)')

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s ' + str(g_version))

    pargs = parser.parse_args(args)

    return pargs


def naturalise_type_of(item):
    """Helper method to convert items to numeric types, where possible.

    This allows such items to behave "correctly" when sorted
    (i.e. numerically rather than lexicographically).
    """
    try:
        return int(item)
    except ValueError:
        try:
            return float(item)
        except ValueError:
            return item


class TableViewerWindow(wx.Frame,
                        listmix.ColumnSorterMixin):

    def __init__(self, data):
        wx.Frame.__init__(self, None, title="Table Viewer", size=(400, 300))

        self.CreateStatusBar()  # A StatusBar in the bottom of the window

        self.populate_from(data)

        # Now that the list exists we can init the other base class,
        # see wx/lib/mixins/listctrl.py
        listmix.ColumnSorterMixin.__init__(self, len(self.itemDataMap[0]))

        self.Show(True)

    def populate_from(self, data):
        self._build_itemDataMap(data)
        self._populate_from_itemDataMap()

    def _build_itemDataMap(self, data):
        """NB: This mapping is needed to support column-sorting."""

        # Local helper function (weird, I know!)
        def cleanup(row):
            return [naturalise_type_of(item) for item in row]

        # itemDataMap is a dict of "arbitrary" IDs to rows,
        # where a row is a seq of items.
        #
        # I will use ID 0 to represent the column headings themselves,
        # and ID 1 will therefore map to "row 0" inside the listCtrl itself
        self.itemDataMap = {rownum: cleanup(row) for rownum, row in enumerate(data)}

    def _populate_from_itemDataMap(self):

        # Create a new ListCtrl instance (and implicitly clear out any old stuff)
        self.listCtrl = wx.ListCtrl(self, style=wx.LC_REPORT | wx.SUNKEN_BORDER)

        for rownum in range(len(self.itemDataMap)):  # iterate rows in order
            row = self.itemDataMap[rownum]
            self._populate_row(rownum, row)

        for colnum, _ in enumerate(self.itemDataMap[0]):
            self.listCtrl.SetColumnWidth(colnum, wx.LIST_AUTOSIZE_USEHEADER)

    def _populate_row(self, rownum, row):
        # ListCtrl's indices don't include the header row,
        # so I must compensate for that here.
        index = rownum - 1

        for colnum, item in enumerate(row):
            if rownum == 0:
                # I treat row zero as being the "header row",
                # which defines the columns, rather than the "content"
                self._create_column(colnum, item)
            elif colnum == 0:
                self.listCtrl.InsertStringItem(index, str(item))
                self.listCtrl.SetItemData(index, rownum)
            else:
                self.listCtrl.SetStringItem(index, colnum, str(item))

    def _create_column(self, colnum, item):
        # Windows appears to have an absurd bug whereby column formatting
        # is ignored for "column 0". Offsetting the column index in here
        # (but nowhere else!) seems to be an effective work-around!
        #
        # see http://stackoverflow.com/questions/3059781/
        #     cannot-format-first-column-in-wxpythons-listctrl
        self.listCtrl.InsertColumn(colnum+1, str(item), format=wx.LIST_FORMAT_RIGHT)

    # Needed by the ColumnSorterMixin, see wx/lib/mixins/listctrl.py
    def GetListCtrl(self):
        return self.listCtrl


def main(args):
    pargs = parse_command_args(args)
    infile = open(pargs.infilename, 'r') if pargs.infilename else sys.stdin
    reader = csv.reader(infile)
    app = wx.App(False)
    frame = TableViewerWindow(reader)
    frame.Show()
    app.MainLoop()


if __name__ == "__main__":
    main(sys.argv[1:])

