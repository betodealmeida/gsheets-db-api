from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from moz_sql_parser import parse, format
from six import string_types

from gsheetsdb.exceptions import NotSupportedError


def find_value_clauses(json):
    if isinstance(json, dict):
        if 'value' in json:
            yield json
        else:
            for obj in json.values():
                for value in find_value_clauses(obj):
                    yield value
    elif isinstance(json, list):
        for element in json:
            for value in find_value_clauses(element):
                yield value


def replace_value(json, replacement_map):
    for key, value in json.items():
        if isinstance(value, dict):
            replace_value(value, replacement_map)
        elif value in replacement_map:
            json[key] = replacement_map[value]


def translate(sql, column_map):
    parsed_query = parse(sql)

    # HAVING is not supported
    if 'having' in parsed_query:
        raise NotSupportedError('HAVING not supported')

    from_ = parsed_query.pop('from')
    if not isinstance(from_, string_types):
        raise NotSupportedError('FROM should be a URL')

    for clause in find_value_clauses(parsed_query):
        replace_value(clause, column_map)

    return format(parsed_query)
