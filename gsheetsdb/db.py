from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import datetime
from enum import Enum
import itertools
import json
import re

from six import string_types
from six.moves.urllib import parse

from moz_sql_parser import parse as parse_sql
import requests

from gsheetsdb.exceptions import Error, NotSupportedError, ProgrammingError
from gsheetsdb.translator import extract_column_aliases, translate


# the JSON payloads has this in the beginning
LEADING = ")]}'\n"

# regex for extracting position of syntax errors
POSITION = re.compile('at line (?P<line>\d+), column (?P<column>\d+)')


class Type(Enum):
    STRING = 'string'
    NUMBER = 'number'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    TIMEOFDAY = 'timeofday'


def connect():
    """
    Constructor for creating a connection to the database.

        >>> conn = connect()
        >>> curs = conn.cursor()

    """
    return Connection()


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


def get_description_from_result(cols):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            col['label'],               # name
            Type(col['type'].lower()),  # type_code
            None,                       # [display_size]
            None,                       # [internal_size]
            None,                       # [precision]
            None,                       # [scale]
            True,                       # [null_ok]
        )
        for col in cols
    ]


def format_error(query, translated_query, errors):
    error_messages = []
    for error in errors:
        detailed_message = error['detailed_message']
        match = POSITION.search(detailed_message)
        if match:
            groups = match.groupdict()
            line = int(groups['line'])
            column = int(groups['column'])

            msg = translated_query.split('\n')[:line]
            msg.append('{indent}^'.format(indent=' ' * (column - 1)))
            msg.append(detailed_message)
            error_messages.append('\n'.join(msg))
        else:
            error_messages.append(detailed_message)

    return """
Original query:
{query}

Translated query:
{translated_query}

Error{plural}:
{error_messages}
    """.format(
        query=query,
        translated_query=translated_query,
        plural='s' if len(errors) > 1 else '',
        error_messages='\n'.join(error_messages),
    ).strip()


def get_url(url, headers=0, gid=0, sheet=None):
    parts = parse.urlparse(url)
    if parts.path.endswith('/edit'):
        path = parts.path[:-len('/edit')]
    else:
        path = parts.path
    path = '/'.join((path.rstrip('/'), 'gviz/tq'))

    qs = parse.parse_qs(parts.query)
    if 'headers' in qs:
        headers = int(qs['headers'][-1])
    if 'gid' in qs:
        gid = qs['gid'][0]

    if parts.fragment.startswith('gid='):
        gid = parts.fragment[len('gid='):]

    args = {}
    if headers > 0:
        args['headers'] = headers
    if sheet is not None:
        args['sheet'] = sheet
    else:
        args['gid'] = gid
    params = parse.urlencode(args)

    return parse.urlunparse(
        (parts.scheme, parts.netloc, path, None, params, None))


def extract_url(sql):
    parsed_query = parse_sql(sql)
    return parsed_query['from']


def get_column_map(url):
    query = 'SELECT * LIMIT 0'
    result = run_query(url, query)
    return {col['label']: col['id'] for col in result['table']['cols']}


def run_query(baseurl, query):
    url = '{baseurl}&tq={query}'.format(
        baseurl=baseurl, query=parse.quote(query, safe='/()'))
    headers = {'X-DataSource-Auth': 'true'}
    r = requests.get(url, headers=headers)
    if r.encoding is None:
        r.encoding = 'utf-8'

    # raise any error messages
    if r.status_code != 200:
        raise ProgrammingError(r.text)

    if r.text.startswith(LEADING):
        result = json.loads(r.text[len(LEADING):])
    else:
        result = r.json()

    return result


def parse_datetime(v):
    """Parse a string like 'Date(2018,0,1,0,0,0)'"""
    if v is None:
        return None

    args = [int(number) for number in v[len('Date('):-1].split(',')]
    args[1] += 1  # month is zero indexed in the response
    return datetime.datetime(*args)


def parse_date(v):
    """Parse a string like 'Date(2018,0,1)'"""
    if v is None:
        return None

    args = [int(number) for number in v[len('Date('):-1].split(',')]
    args[1] += 1  # month is zero indexed in the response
    return datetime.date(*args)


def parse_timeofday(v):
    if v is None:
        return None

    return datetime.time(*v)


processors = {
    'string': lambda v: v,
    'number': lambda v: v,
    'boolean': lambda v: v,
    'date': parse_date,
    'datetime': parse_datetime,
    'timeofday': parse_timeofday,
}


def process_rows(cols, rows):
    Row = namedtuple(
        'Row',
        [col['label'].replace(' ', '_') for col in cols],
        rename=True)

    results = []
    for row in rows:
        values = []
        for i, col in enumerate(row['c']):
            processor = processors[cols[i]['type']]
            values.append(processor(col['v']) if col else None)
        results.append(Row(*values))

    return results


class Connection(object):

    """Connection to a Google Spreadsheet."""

    def __init__(self):
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
        cursor = Cursor()
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None, headers=0):
        cursor = self.cursor()
        return cursor.execute(operation, parameters, headers)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Cursor(object):

    """Connection cursor."""

    def __init__(self):
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

        # fetch aliases, since they will be removed by the translator
        aliases = extract_column_aliases(query)

        # extract URL from the `FROM` clause
        from_ = extract_url(query)
        baseurl = get_url(from_, headers)

        # translate colum names to ids
        column_map = get_column_map(baseurl)
        translated_query = translate(query, column_map)

        result = run_query(baseurl, translated_query)

        if result['status'] == 'error':
            raise ProgrammingError(
                format_error(query, translated_query, result['errors']))

        cols = result['table']['cols']
        for alias, col in zip(aliases, cols):
            if alias is not None:
                col['label'] = alias
        self.description = get_description_from_result(cols)

        rows = result['table']['rows']
        self._results = process_rows(cols, rows)

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
