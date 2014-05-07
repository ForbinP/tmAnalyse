#! /usr/bin/env python

"""
Export (or 'dump') specified tables from an SQLite DB as CSV data.

Intended for use with Telemetry ('tm') files, which benefit from some
custom handling.
"""

import sys
import argparse
import csv

import tmDumpCore
import tmDumpTelemetry

g_version = 0.1

g_description = ('''
Export (or 'dump') specified tables from an SQLite DB as CSV data.

Intended for use with Telemetry ('tm') files, which benefit from some
custom handling.

Designed for Telemetry 2 (http://www.radgametools.com/telemetry.htm)

v{}
'''.format(g_version))

g_verbose = False


def parse_command_args(cargs):

    parser = argparse.ArgumentParser(
        description=g_description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        'infilename',
        help='SQLite file to work from')

    parser.add_argument(
        '-l', '--list-tables',
        help='list all available tables in the DB',
        action='store_true')

    parser.add_argument(
        'table', nargs='?',  # '?' => optional param
        help='name of the table to dump')

    parser.add_argument(
        '-z', '--zones',
        help='dump telemetry zones info with custom handling',
        action='store_true')

    parser.add_argument(
        '-t', '--zone-totals',
        help='dump telemetry zones with totals per zone',
        action='store_true')

    parser.add_argument(
        '-e', '--zone-totals-excl',
        help='dump telemetry zones with *exclusive* totals per zone',
        action='store_true')

    parser.add_argument(
        '-v', '--verbose',
        help='include verbose diagnostics',
        action='store_true')

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s ' + str(g_version))

    pargs = parser.parse_args(cargs)

    global g_verbose
    g_verbose = pargs.verbose

    verbose('parsed args =', pargs)

    return pargs


def verbose(*a):
    """Print my args, but only if we're in verbose mode."""
    if g_verbose:
        for arg in a:
            print arg,
        print


def main(cargs):
    pargs = parse_command_args(cargs)

    # Extract relevant params from the parsed args
    infilename = pargs.infilename
    table = pargs.table

    conn = tmDumpCore.create_db_connection(infilename)

    csv_file = sys.stdout
    writer = csv.writer(csv_file)

    if pargs.list_tables:
        tmDumpCore.list_tables(conn, writer)
    elif pargs.zones:
        tmDumpTelemetry.dump_zones(conn, writer)
    elif pargs.zone_totals:
        tmDumpTelemetry.dump_zone_totals(conn, writer)
    elif pargs.zone_totals_excl:
        tmDumpTelemetry.dump_zone_totals_exclusive(conn, writer)
    elif table is None:
        print('No table name specified!')
    else:
        tmDumpCore.dump_table(conn, table, writer)

    conn.close()


if __name__ == '__main__':
    main(sys.argv[1:])
