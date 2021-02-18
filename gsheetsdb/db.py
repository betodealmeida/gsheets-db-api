from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from six import string_types

from gsheetsdb.exceptions import Error, NotSupportedError, ProgrammingError
from gsheetsdb.query import execute
from gsheetsdb.sqlite import execute as sqlite_execute

import sqlparse
from gsheetsdb.url import url_from_sql
from gsheetsdb.sqlite import execute_all_sql

logger = logging.getLogger(__name__)


def connect(credentials=None):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect()
        >>> curs = conn.cursor()

    """
    return Connection(credentials)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Error(
                '{klass} already closed'.format(klass=self.__class__.__name__))
        return f(self, *args, **kwargs)
    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise Error('Called before `execute`')
        return f(self, *args, **kwargs)
    return g


class Connection(object):

    """Connection to a Google Spreadsheet."""

    def __init__(self, credentials=None):
        self.credentials = credentials

        self.closed = False
        self.cursors = []

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.credentials)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None, headers=0):
        cursor = self.cursor()
        return cursor.execute(operation, parameters, headers)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.commit()  # no-op
        self.close()


class Cursor(object):

    """Connection cursor."""

    def __init__(self, credentials=None):
        self.credentials = credentials

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to a list of rows after a successful query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self):
        return len(self._results)

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None, headers=0):

        self.description = None
        query = apply_parameters(operation, parameters or {})

        # Use switch cases for select, insert, update, delete (JG)
        # Parse query to extract SQL statement/query type
        parsed = sqlparse.parse(query)[0]
        parsed_token = parsed.tokens

        # Execute only for 'SELECT' query
        if str(parsed_token[0]) == 'SELECT':
            try:
                self._results, self.description = execute(
                query, headers, self.credentials)

            except (ProgrammingError, NotSupportedError):
                logger.info('Query failed, running in SQLite')
                self._results, self.description = sqlite_execute(
                    query, headers, self.credentials)

        # Execute when statement is other than 'SELECT'
        else:
            # Execute all SQL statements other than 'SELECT'
            execute_all_sql(query, headers, self.credentials)
            exit()

        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        out = self._results[:size]
        self._results = self._results[size:]
        return out

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        out = self._results[:]
        self._results = []
        return out

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return iter(self._results)


def apply_parameters(operation, parameters):
    escaped_parameters = {
        key: escape(value) for key, value in parameters.items()
    }
    return operation % escaped_parameters


def escape(value):
    if value == '*':
        return value
    elif isinstance(value, string_types):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (list, tuple)):
        return '({0})'.format(', '.join(escape(element) for element in value))
