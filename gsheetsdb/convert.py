from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import datetime


def parse_datetime(v):
    """Parse a string like 'Date(2018,0,1,0,0,0)'"""
    args = [int(number) for number in v[len('Date('):-1].split(',')]
    args[1] += 1  # month is zero indexed in the response
    return datetime.datetime(*args)


def parse_date(v):
    """Parse a string like 'Date(2018,0,1)'"""
    args = [int(number) for number in v[len('Date('):-1].split(',')]
    args[1] += 1  # month is zero indexed in the response
    return datetime.date(*args)


def parse_timeofday(v):
    return datetime.time(*v)


converters = {
    'string': lambda v: v,
    'number': lambda v: v,
    'boolean': lambda v: v,
    'date': parse_date,
    'datetime': parse_datetime,
    'timeofday': parse_timeofday,
}


def convert_rows(cols, rows):
    Row = namedtuple(
        'Row',
        [col['label'].replace(' ', '_') for col in cols],
        rename=True)

    results = []
    for row in rows:
        values = []
        for i, col in enumerate(row['c']):
            converter = converters[cols[i]['type']]
            values.append(converter(col['v']) if col else None)
        results.append(Row(*values))

    return results
