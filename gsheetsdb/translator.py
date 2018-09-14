from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from moz_sql_parser import parse, format


def find_value_clauses(json):
    if isinstance(json, dict):
        if 'value' in json:
            yield json
        else:
            for value in json.values():
                yield from find_value_clauses(value)
    elif isinstance(json, list):
        for element in json:
            yield from find_value_clauses(element)


def replace_value(json, replacement_map):
    for key, value in json.items():
        if isinstance(value, dict):
            replace_value(value, replacement_map)
        elif value in replacement_map:
            json[key] = replacement_map[value]


def translate(sql, column_map):
    parsed_query = parse(sql)

    # check if it's a url, otherwise raise exception
    from_ = parsed_query.pop('from')

    for clause in find_value_clauses(parsed_query):
        replace_value(clause, column_map)

    return format(parsed_query)
