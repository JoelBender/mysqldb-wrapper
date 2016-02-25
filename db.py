#!/usr/bin/python

"""
MySQLdb Wrapper
===============
"""

import sys
import threading

from time import time as _time

import MySQLdb
import MySQLdb.cursors

from bacpypes.debugging import bacpypes_debugging, ModuleLogger

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# connection pool keyword arguments should at least contain 'host' and 'db'
db_connection_kwargs = {}
db_connection_timeout = 600.0   # after ten minutes, close the connection
db_connection_pool = {}
db_connection_activity = {}
db_connection_pool_lock = threading.Lock()
db_cursor_class = MySQLdb.cursors.DictCursor


@bacpypes_debugging
def get_connection():
    """Maintain a pool of per-thread database connections."""
    global db_connection_pool, db_connection_activity
    if _debug: get_connection._debug("get_connection")

    # lock the connection pool
    db_connection_pool_lock.acquire()

    # get our thread
    current_thread = threading.currentThread()
    if _debug: get_connection._debug("    - current_thread: %r", current_thread)

    try:
        # clean out idle connections and dead threads
        now = _time()
        timeout = now - db_connection_timeout
        for th, thtime in db_connection_activity.items():
            if not th.isAlive():
                if _debug: get_connection._debug("    - del: %r", th)

                # close the connection and delete it
                db_connection_pool[th].close()
                del db_connection_pool[th]
                del db_connection_activity[th]
            elif thtime < timeout:
                if _debug: get_connection._debug("    - idle: %r", th)

                db_connection_pool[th].close()
                del db_connection_pool[th]
                del db_connection_activity[th]

        # if this thread already has a connection, return it
        if current_thread in db_connection_pool:
            db_connection = db_connection_pool[current_thread]
            while db_connection:
                if _debug: get_connection._debug("    - cached connection: %r", db_connection)

                try:
                    # still OK?
                    db_connection.ping()
                    break

                except MySQLdb.OperationalError as err:
                    _log.exception("ping failed: %r", err)

                    # create a new connection, add it to the pool, and return it
                    db_connection = MySQLdb.connect(**db_connection_kwargs)
                    if not db_connection:
                        _log.error("failed to connect")
                        sys.exit(1)

                    db_connection_pool[current_thread] = db_connection
        else:
            # create a new connection, add it to the pool, and return it
            db_connection = MySQLdb.connect(**db_connection_kwargs)
            if not db_connection:
                _log.error("failed to connect")
                sys.exit(1)

            db_connection_pool[current_thread] = db_connection

        # update the activity
        db_connection_activity[current_thread] = now

    finally:
        pass

    # release the connection pool
    db_connection_pool_lock.release()

    # return the connection
    return db_connection


@bacpypes_debugging
def execute(*args, **kwargs):
    """Execute a MySQL statement."""
    if _debug: execute._debug("execute %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor(cursorclass=db_cursor_class)
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor(cursorclass=db_cursor_class)
    if _debug: execute._debug("    - cursor: %r", cursor)

    rslt = cursor.execute(*args, **kwargs)
    if _debug: execute._debug("    - rslt: %r", rslt)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()

    # might return a count
    return rslt


@bacpypes_debugging
def fetch_one(*args, **kwargs):
    """Fetch one row from the database."""
    if _debug: fetch_one._debug("fetch_one %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor(cursorclass=db_cursor_class)
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor(cursorclass=db_cursor_class)
    if _debug: fetch_one._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args, **kwargs)
    if _debug: fetch_one._debug("    - count: %r", count)

    rslt = cursor.fetchone()
    if _debug: fetch_one._debug("    - rslt: %r", rslt)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()

    # might return a count
    return rslt


@bacpypes_debugging
def fetch_all(*args, **kwargs):
    """Fetch all of the rows from the database and return them as a list."""
    if _debug: fetch_all._debug("fetch_all %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor(cursorclass=db_cursor_class)
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor(cursorclass=db_cursor_class)
    if _debug: fetch_all._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args, **kwargs)
    if _debug: fetch_all._debug("    - count: %r", count)

    rslt = []
    for i in range(count):
        row = cursor.fetchone()
        if _debug: fetch_all._debug("    - row: %r", row)

        rslt.append(row)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()

    # return the list
    return rslt


@bacpypes_debugging
def yield_rows(*args, **kwargs):
    """Generator function to yield rows from the database, used when the
    result set is very large."""
    if _debug: yield_rows._debug("yield_rows %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor(cursorclass=db_cursor_class)
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor(cursorclass=db_cursor_class)
    if _debug: yield_rows._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args, **kwargs)
    if _debug: yield_rows._debug("    - count: %r", count)

    for i in range(count):
        row = cursor.fetchone()
        if _debug: yield_rows._debug("    - row: %r", row)

        yield row

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()


@bacpypes_debugging
def yield_values(*args, **kwargs):
    """Generator function to yield values from the database, used when the
    result set is very large."""
    if _debug: yield_values._debug("yield_values %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor()
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor()
    if _debug: yield_values._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args, **kwargs)
    if _debug: yield_values._debug("    - count: %r", count)

    for i in range(count):
        row = cursor.fetchone()
        if len(row) == 1:
            value = row[0]
        else:
            value = row
        if _debug: fetch_values._debug("    - value: %r", value)

        yield value

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()


def dict2obj(d):
    """Given a dictionary, return an object with the keys mapped to attributes
    and the values mapped to attribute values.  This is recursive, so nested
    dictionaries are nested objects."""
    top = type('dict2obj', (object,), d)
    seqs = tuple, list, set, frozenset
    for k, v in d.items():
        if isinstance(v, dict):
            setattr(
                top,
                k, dict2obj(v)
                )
        elif isinstance(v, seqs):
            setattr(
                top,
                k, type(v)(dict2obj(sj) if isinstance(sj, dict) else sj for sj in v)
                )
        else:
            setattr(top, k, v)
    return top


@bacpypes_debugging
def yield_objects(*args, **kwargs):
    """Yield each of the rows from the database as an object.  The column names
    become attribute names and the column values become attribute values."""
    if _debug: yield_objects._debug("yield_objects %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor(cursorclass=db_cursor_class)
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor(cursorclass=db_cursor_class)
    if _debug: yield_objects._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args, **kwargs)
    if _debug: yield_objects._debug("    - count: %r", count)

    for i in range(count):
        row = cursor.fetchone()
        if _debug: yield_objects._debug("    - row: %r", row)

        yield dict2obj(row)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()


@bacpypes_debugging
def fetch_n(n, *args, **kwargs):
    """Fetch at most n rows of data from the database."""
    if _debug: fetch_n._debug("fetch_n %r %r", n, args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor(cursorclass=db_cursor_class)
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor(cursorclass=db_cursor_class)
    if _debug: fetch_n._debug("    - cursor: %r", cursor)

    rslt = []

    count = cursor.execute(*args, **kwargs)
    if _debug: fetch_n._debug("    - count: %r", count)

    if count > n: count = n

    for i in range(count):
        row = cursor.fetchone()
        if _debug: fetch_n._debug("    - row: %r", row)

        rslt.append(row)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()

    return rslt


@bacpypes_debugging
def fetch_value(*args, **kwargs):
    """Fetch a single value from the database."""
    if _debug: fetch_value._debug("fetch_value %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor()
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor()
    if _debug: fetch_value._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args)
    if _debug: fetch_value._debug("    - count: %r", count)

    if count == 0:
        value = None
    else:
        row = cursor.fetchone()
        if len(row) == 1:
            value = row[0]
        else:
            value = row
        if _debug: fetch_value._debug("    - value: %r", value)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()

    # might return a count
    return value


@bacpypes_debugging
def fetch_values(*args, **kwargs):
    """Fetch multiple values from the database."""
    if _debug: fetch_values._debug("fetch_values %r %r", args, kwargs)

    # get a cursor
    local_cursor = None
    if 'connection' in kwargs:
        local_cursor = cursor = kwargs['connection'].cursor()
        del kwargs['connection']
    elif 'cursor' in kwargs:
        cursor = kwargs['cursor']
        del kwargs['cursor']
    else:
        local_cursor = cursor = get_connection().cursor()
    if _debug: fetch_values._debug("    - cursor: %r", cursor)

    count = cursor.execute(*args)
    if _debug: fetch_values._debug("    - count: %r", count)

    valueList = []
    for i in range(count):
        row = cursor.fetchone()
        if len(row) == 1:
            value = row[0]
        else:
            value = row
        if _debug: fetch_values._debug("    - value: %r", value)

        valueList.append(value)

    # all done with the local cursor
    if local_cursor:
        local_cursor.close()

    # might return a count
    return valueList


@bacpypes_debugging
def close_connections():
    """Close the connections to the database."""
    if _debug: close_connections._debug("close_connections")

    for t, c in db_connection_pool.items():
        if _debug and t.isAlive():
            close_connections._warning("%r is still alive and its database connection is closing", t)
        c.close()


def escape(s):
    """Escape the provided string for the db."""
    return MySQLdb.escape_string(s)
