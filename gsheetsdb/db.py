from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
from enum import Enum
import itertools
import json

from six import string_types
from six.moves.urllib import parse

import requests

from gsheetsdb import exceptions


# the JSON payloads has this in the beginning
LEADING = ")]}'\n"


class Type(Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3
    DATE = 4
    DATETIME = 5
    TIMEOFDAY = 6


def connect(url, headers=0, gid=0, sheet=None):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8082)
        >>> curs = conn.cursor()

    """
    return Connection(url, headers, gid, sheet)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(
                '{klass} already closed'.format(klass=self.__class__.__name__))
        return f(self, *args, **kwargs)
    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise exceptions.Error('Called before `execute`')
        return f(self, *args, **kwargs)
    return g


def get_description_from_result(result):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            col['label'],               # name
            Type[col['type'].upper()],  # type_code
            None,                       # [display_size]
            None,                       # [internal_size]
            None,                       # [precision]
            None,                       # [scale]
            True,                       # [null_ok]
        )
        for col in result['table']['cols']
    ]


class Connection(object):

    """Connection to a Google Spreadsheet."""

    def __init__(self, url, headers=0, gid=0, sheet=None):
        args = {}
        if headers > 0:
            args['headers'] = headers

        if sheet is not None:
            args['sheet'] = sheet
        else:
            args['gid'] = gid

        parts = parse.urlparse(url)
        path = '/'.join((parts.path.rstrip('/'), 'gviz/tq'))
        params = parse.urlencode(args)
        self.baseurl = parse.urlunparse(
            (parts.scheme, parts.netloc, path, None, params, None))

        self.closed = False
        self.cursors = []

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except exceptions.Error:
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
        cursor = Cursor(self.baseurl)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Cursor(object):

    """Connection cursor."""

    def __init__(self, baseurl):
        self.baseurl = baseurl

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
    def execute(self, operation, parameters=None):
# curl --header "X-DataSource-Auth:true" 'https://docs.google.com/spreadsheets/d/1q9REzifHb90vewm4XMjnWFKOPNTcG6Xh8s6Hwo9OpFo/gviz/tq?gid=0&headers=2&tq=select%20A%2C%20sum(B)%20group%20by%20A'
# {"version":"0.6","reqId":"0","status":"ok","sig":"850295587","table":{"cols":[{"id":"A","label":"country string","type":"string"},{"id":"sum-B","label":"sum cnt number","type":"number"}],"rows":[{"c":[{"v":"BR"},{"v":4.0}]},{"c":[{"v":"IN"},{"v":5.0}]}]}}
        self.description = None

        query = apply_parameters(operation, parameters or {})
        url = '{baseurl}&tq={query}'.format(
            baseurl=self.baseurl, query=parse.quote(query, safe='/()'))
        print(url)
        headers = {'X-DataSource-Auth': 'true'}
        r = requests.get(url, headers=headers)
        if r.encoding is None:
            r.encoding = 'utf-8'

        # raise any error messages
        if r.status_code != 200:
            raise exceptions.ProgrammingError(r.text)

        if r.text.startswith(LEADING):
            result = json.loads(r.text[len(LEADING):])
        else:
            result = r.json()

        cols = result['table']['cols']
        rows = result['table']['rows']
        Row = namedtuple('Row', [col['label'] for col in cols], rename=True)
        self._results = [Row(*[col['v'] for col in row['c']]) for row in rows]
        self.description = get_description_from_result(result)

        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self.next()
        except StopIteration:
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
        return list(itertools.islice(self, size))

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self)

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
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (list, tuple)):
        return ', '.join(escape(element) for element in value)
