"""
Core facilities for my tmDump utility.

This handles most of the generic SQLite handling,
and avoids any telemetry-specific logic.
"""

import sys
import os
import sqlite3
import csv

from nose.tools import raises


def create_db_connection(infilename):
    if not os.path.isfile(infilename):
        raise IOError('File not found')

    conn = sqlite3.connect(infilename)

    # The following comment suppresses a bogus warning about row_factory
    # noinspection PyPropertyAccess
    conn.row_factory = sqlite3.Row  # a 'better' row factory, supporting name-lookup

    verify_db_connection(conn)
    return conn


class Test_create_db_connection(object):
    def test_with_test_db(self):
        create_db_connection('test.db')

    @raises(sqlite3.DatabaseError)
    def test_with_non_db_file(self):
        create_db_connection('not-a-db-file')

    @raises(IOError)
    def test_with_non_existent_file(self):
        create_db_connection('not-existing-file')


def verify_db_connection(conn):
    try:
        # Try some generic query which should work for all DBs
        conn.execute(
            "SELECT * FROM sqlite_master WHERE type='table'"
        )
    except sqlite3.DatabaseError:
        print "DB doesn't exist, or isn't readable!"
        print
        print 'If an apparently valid DB file is failing to be read, then '
        print 'You probably need to upgrade your sqlite3.dll (for FTS4 support)'
        print
        print 'NB: This Python instance is running from:'
        print sys.executable
        print

        raise


def list_tables(conn, csv_writer):
    cur = conn.execute('''
        SELECT * FROM sqlite_master WHERE type='table'
    ''')

    for row in cur:
        seq = [row['name']]
        csv_writer.writerow(seq)


def sqlite_row_to_seq(row):
    """
    Covert a database `row` into a proper sequence type that I can iterate over.

    I need this to work-around a bug whereby `sqlite3.Row` doesn't act as a sequence. :-(
    See: http://bugs.python.org/issue10203
    """
    return [row[n] for n in range(len(row))]


def write_from_cursor(cur, csv_writer):
    csv_writer.writerow([d[0] for d in cur.description])
    for row in cur:
        seq = sqlite_row_to_seq(row)
        csv_writer.writerow(seq)


def dump_table(conn, table, csv_writer):
    """
    Dump the specified table as CSV

    :param conn: connection object to the DB we are using
    :param table: name of the table to dump
    :param csv_writer: output goes to this csvwriter object
    """
    cur = conn.execute('''
        SELECT * FROM {}
    '''.format(table))

    write_from_cursor(cur, csv_writer)


class TestTableDumping(object):
    def setup(self):
        self.conn = create_db_connection('test.db')
        csv_file = sys.stdout
        self.csv_writer = csv.writer(csv_file)

    def teardown(self):
        self.conn.close()

    def check_output_matches(self, target):
        output = sys.stdout.getvalue().strip()
        assert output.split() == target.split()

    def test_dump_tab1(self):
        dump_table(self.conn, 'tab1', self.csv_writer)
        target = '''
            col1,col2
            1,one
            2,two
            3,three
        '''
        self.check_output_matches(target)

    def test_dump_tab2(self):
        dump_table(self.conn, 'tab2', self.csv_writer)
        target = '''
            col1,col2,col3
            1,one,ONE
            2,two,TWO
            3,three,THREE
        '''
        self.check_output_matches(target)

    def test_list_tables(self):
        list_tables(self.conn, self.csv_writer)
        target = '''
            tab1
            tab2
        '''
        self.check_output_matches(target)
