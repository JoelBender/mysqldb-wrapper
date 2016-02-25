#!/usr/bin/python

"""
Test MySQLdb Wrapper
====================
"""

import random

from bacpypes.debugging import bacpypes_debugging, ModuleLogger
from bacpypes.consolelogging import ArgumentParser

import db

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# settings
default_db_host = "localhost"
default_db_db = "testdb"


@bacpypes_debugging
def drop_test_table():
    if _debug: drop_test_table._debug("drop_test_table")

    db.execute("""DROP TABLE IF EXISTS `TestTable`""")
    if _debug: drop_test_table._debug("    - table dropped")

@bacpypes_debugging
def create_test_table():
    if _debug: create_test_table._debug("create_test_table")

    create_table_command = """
        CREATE TABLE `TestTable` (
          `row_int` int(10) unsigned NOT NULL DEFAULT '0',
          `row_str` varchar(16) DEFAULT NULL,
          PRIMARY KEY (`row_int`)
        ) ENGINE=MyISAM DEFAULT CHARSET=latin1;
        """
    db.execute(create_table_command)
    if _debug: create_test_table._debug("    - table created")

def main():
    # build a parser
    parser = ArgumentParser(description=__doc__)

    # add a --host option to override the default host address
    parser.add_argument(
        '--host', type=str,
        default=default_db_host,
        help='database host address',
        )

    # add a --port option to override the default database
    parser.add_argument(
        '--db', type=str,
        default=default_db_db,
        help='database',
        )

    # parse the command line arguments
    args = parser.parse_args()

    if _debug: _log.debug("initialization")
    if _debug: _log.debug("    - args: %r", args)

    # give the connection information to the db module
    db.db_connection_kwargs = {
        'host': args.host,
        'db': args.db,
        }
    if _debug: _log.debug("    - db_connection_kwargs: %r", db.db_connection_kwargs)

    # create the test table
    create_test_table()

    # test data
    test_data = [
        (100, "a"),
        (110, "aa"),
        (111, "aaa"),
        (112, "aab"),
        (120, "ab"),
        (200, "b"),
        (210, "ba"),
        (211, "baa"),
        (212, "bab"),
        (220, "bb"),
        ]

    # insert the data
    for row_int, row_str in test_data:
        db.execute("insert into TestTable values (%s, %s)", (row_int, row_str))

    # count the number of rows
    row_count = db.fetch_value("select count(*) from TestTable")
    if _debug: _log.debug("    - row_count: %r", row_count)
    assert row_count == len(test_data)

    # test for some specific values
    for row_int, row_str in random.sample(test_data, 3):
        if _debug: _log.debug("    - row_int, row_str: %r, %r", row_int, row_str)
        row_value = db.fetch_value("select row_int from TestTable where row_str = %s", (row_str,))
        assert row_value == row_int

    # get some values
    row_ints = db.fetch_values("select row_int from TestTable where row_int < 200 order by row_int")
    if _debug: _log.debug("    - row_ints: %r", row_ints)
    assert row_ints == [100, 110, 111, 112, 120]

    # yield some objects
    for row_object in db.yield_objects("select * from TestTable where row_str >= %s", ('b',)):
        if _debug: _log.debug("    - row_object: %r, row_int=%r, row_str=%r", row_object, row_object.row_int, row_object.row_str)

    # drop the test table
    drop_test_table()

    # close the connections
    db.close_connections()

    if _debug: _log.debug("finally")

if __name__ == "__main__":
    main()
