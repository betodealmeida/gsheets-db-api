from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import warnings

from six import string_types


class JSONMatcher:
    
    def __init__(self, json):
        self.json = json

    def match(self, other):
        raise NotImplementedError('Subclasses should implement `match`')


class SubsetMatcher(JSONMatcher):

    def match(self, other):
       return is_subset(self.json, other) 


def is_subset(json, other):
    if isinstance(json, list):
        if not isinstance(other, list):
            return False

        for value in json:
            if not is_subset(value, other):
                return False
        return True

    elif isinstance(json, dict):
        if isinstance(other, string_types):
            return False
        elif isinstance(other, dict):
            other = [other]

        for k, v in json.items():
            # each value should be a subset of the value in other
            match = False
            for option in other:
                if is_subset(v, option.get(k)):
                    break
            else:
                return False

        return True

    else:
        return json == other


class Processor:
    
    pattern = lambda parsed_query: False

    @classmethod
    def match(cls, parsed_query):
        return cls.pattern.match(parsed_query)

    def pre_process(self, parsed_query, column_map):
        raise NotImplementedError('Subclasses should implement `pre_process`')

    def post_process(self, payload, aliases):
        raise NotImplementedError('Subclasses should implement `post_process`')


class CountStar(Processor):

    pattern = SubsetMatcher({'select': {'value': {'count': '*'}}})

    def pre_process(self, parsed_query, column_map):
        warnings.warn(
            'COUNT(*) only works if at least one column has no NULLs')

        select = parsed_query['select']
        if not isinstance(select, list):
            select = [select]

        new_select = []
        for expr in select:
            if isinstance(expr, dict) and expr['value'] == {'count': '*'}:
                self.alias = expr.get('name', 'count star')
            else:
                new_select.append(expr)

        # count each column
        for label in column_map:
            alias = '__{namespace}__{label}'.format(
                namespace=self.__class__.__name__, label=label)
            new_select.append({'value': {'count': label}, 'name': alias})

        parsed_query['select'] = new_select
        return parsed_query 

    def post_process(self, payload, aliases):
        added_columns = [
            alias.startswith('__{0}__'.format(self.__class__.__name__))
            for alias in aliases
        ]
        payload['table']['cols'] = [
            col for (col, added)
            in zip(payload['table']['cols'], added_columns)
            if not added
        ]
        payload['table']['cols'].append(
            {'id': 'count-star', 'label': self.alias, 'type': 'number'})

        for row in payload['table']['rows']:
            values = [
                value for (value, added) in zip(row['c'], added_columns)
                if added
            ]
            count_star = max(col['v'] for col in values)
            row['c'] = [
                value for (value, added) in zip(row['c'], added_columns)
                if not added
            ]
            row['c'].append({'v': count_star})

        # the API returns no rows when the count is zero
        if not payload['table']['rows']:
            payload['table']['rows'].append({'c': [{'v': 0}]})

        return payload


processors = [CountStar]
