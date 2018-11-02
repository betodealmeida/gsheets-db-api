from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import warnings

from six import string_types


GRANULARITIES = [
    'year',
    'month',
    'day',
    'hour',
    'minute',
    'second',
]

LOWER_BOUNDS = [
    None,  # year
    0,     # month
    1,     # day
    0,     # hour
    0,     # minute
    0,     # second
]


class Any:
    def __eq__(self, other):
        return True


class OneOf:
    def __init__(self, valid_values):
        self.valid_values = valid_values

    def __eq__(self, other):
        return other in self.valid_values


class JSONMatcher:

    def __init__(self, json=None):
        self.json = json

    def __getitem__(self, key):
        return self.__class__(self.json[key])

    def match(self, other):
        raise NotImplementedError('Subclasses should implement `match`')


class DummyMatcher(JSONMatcher):

    def match(self, other):
        return False


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
            for option in other:
                if k in option and is_subset(v, option[k]):
                    break
            else:
                return False

        return True

    elif isinstance(other, list):
        return json in other

    else:
        return json == other


class Processor:

    pattern = DummyMatcher()

    @classmethod
    def match(cls, parsed_query):
        return cls.pattern.match(parsed_query)

    def pre_process(self, parsed_query, column_map):
        return parsed_query

    def post_process(self, payload, aliases):
        return payload


class DateTrunc(Processor):

    """
    Implement `datetrunc` UDF.

        sql> SELECT time, datetrunc('month', time) FROM "http://example.com"
        time                 datetrunc-time-month
        -------------------  ----------------------
        2018-09-01 00:00:00  2018-09-01 00:00:00
        2018-09-02 00:00:00  2018-09-01 00:00:00
        2018-09-03 00:00:00  2018-09-01 00:00:00
        2018-09-04 00:00:00  2018-09-01 00:00:00
        2018-09-05 00:00:00  2018-09-01 00:00:00
        2018-09-06 00:00:00  2018-09-01 00:00:00
        2018-09-07 00:00:00  2018-09-01 00:00:00
        2018-09-08 00:00:00  2018-09-01 00:00:00
        2018-09-09 00:00:00  2018-09-01 00:00:00
        sql>

    This works by calling multiple time functions that extract year/month/etc,
    and padding the values below the requested granularity. The query above
    would be translated to:

        SELECT time, year(time), month(time)

    The post-processor then build the new datetime by using the year and month,
    and padding day/time to a lower boundary.

    """

    pattern = SubsetMatcher(
        {
            'select': {
                'value': {
                    'datetrunc': [{'literal': OneOf(GRANULARITIES)}, Any()],
                },
            },
        },
    )

    def pre_process(self, parsed_query, column_map):
        select = parsed_query['select']
        if not isinstance(select, list):
            select = [select]

        # match select
        new_select = []
        matcher = self.pattern['select']
        self.new_columns = []
        for i, expr in enumerate(select):
            if matcher.match(expr):
                alias = expr.get('name')
                self.new_columns.append((i, alias, expr))
                new_select.extend(self.get_columns(expr))
            else:
                new_select.append(expr)

        # remove duplicates
        seen = set()
        deduped_select = []
        for expr in new_select:
            value = expr['value']
            if isinstance(value, dict):
                value = tuple(value.items())
            if value not in seen:
                seen.add(value)
                deduped_select.append(expr)

        # remove columns from group by
        groupby = parsed_query.get('groupby')
        if groupby:
            new_groupby = []
            matcher = SubsetMatcher({'value': {
                'datetrunc': [{'literal': OneOf(GRANULARITIES)}, Any()]},
            })
            if not isinstance(groupby, list):
                groupby = [groupby]
            for expr in groupby:
                if matcher.match(expr):
                    new_groupby.extend(
                        self.get_columns(expr, alias_column=False))
                else:
                    new_groupby.append(expr)

            # remove duplicates
            seen = set()
            deduped_groupby = []
            for expr in new_groupby:
                value = expr['value']
                if isinstance(value, dict):
                    value = tuple(value.items())
                if value not in seen:
                    seen.add(value)
                    deduped_groupby.append(expr)

            parsed_query['groupby'] = deduped_groupby

        parsed_query['select'] = deduped_select
        return parsed_query

    def post_process(self, payload, aliases):
        added_columns = [
            alias and
            alias.startswith('__{0}__'.format(self.__class__.__name__))
            for alias in aliases
        ]

        cols = payload['table']['cols']
        payload['table']['cols'] = [
            col for (col, added) in zip(cols, added_columns) if not added
        ]

        for position, alias, expr in self.new_columns:
            id_ = 'datetrunc-{name}-{granularity}'.format(
                name=expr['value']['datetrunc'][1],
                granularity=expr['value']['datetrunc'][0]['literal'],
            )
            payload['table']['cols'].insert(
                position,
                {'id': id_, 'label': alias or id_, 'type': 'datetime'})

        for row in payload['table']['rows']:
            row_c = row['c']
            row['c'] = [
                value for (value, added) in zip(row['c'], added_columns)
                if not added
            ]
            for position, alias, expr in self.new_columns:
                row['c'].insert(
                    position, {'v': self.get_value(cols, row_c, expr)})

        return payload

    def get_value(self, cols, row_c, expr):
        """
        Build the datetime from individual columns.

        """
        name = expr['value']['datetrunc'][1]
        granularity = expr['value']['datetrunc'][0]['literal']
        i = GRANULARITIES.index(granularity)

        # map function to index
        labels = [col['label'] for col in cols]
        values = []
        for func_name in GRANULARITIES:
            label = '{0}({1})'.format(func_name, name)
            if label in labels:
                values.append(row_c[labels.index(label)]['v'])

        # truncate values to requested granularity and pad with lower bounds
        args = values[:i+1]
        args += LOWER_BOUNDS[len(args):]
        args = [str(int(arg)) for arg in args]

        return 'Date({0})'.format(','.join(args))

    def get_columns(self, expr, alias_column=True):
        """
        Get all columns required to compute a given granularity.

        """
        name = expr['value']['datetrunc'][1]
        granularity = expr['value']['datetrunc'][0]['literal']

        for func_name in GRANULARITIES:
            alias = '__{namespace}__{func_name}__{name}'.format(
                namespace=self.__class__.__name__,
                func_name=func_name,
                name=name,
            )
            column = {'value': {func_name: name}}
            if alias_column:
                column['name'] = alias
            yield column
            if func_name == granularity:
                break


class CountStar(Processor):

    pattern = SubsetMatcher({'select': {'value': {'count': '*'}}})

    def pre_process(self, parsed_query, column_map):
        warnings.warn(
            'COUNT(*) only works if at least one column has no NULLs')

        select = parsed_query['select']
        if not isinstance(select, list):
            select = [select]

        new_select = []
        matcher = self.pattern['select']
        self.new_columns = []
        for i, expr in enumerate(select):
            if matcher.match(expr):
                alias = expr.get('name', 'count star')
                self.new_columns.append((i, alias, expr))
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
            alias and
            alias.startswith('__{0}__'.format(self.__class__.__name__))
            for alias in aliases
        ]

        payload['table']['cols'] = [
            col for (col, added)
            in zip(payload['table']['cols'], added_columns)
            if not added
        ]

        position, alias, expr = self.new_columns[0]
        payload['table']['cols'].insert(
            position, {'id': 'count-star', 'label': alias, 'type': 'number'})

        for row in payload['table']['rows']:
            values = [
                value['v'] for (value, added) in zip(row['c'], added_columns)
                if added
            ]
            count_star = max(values)
            row['c'] = [
                value for (value, added) in zip(row['c'], added_columns)
                if not added
            ]
            row['c'].insert(position, {'v': count_star})

        # the API returns no rows when the count is zero
        if not payload['table']['rows']:
            payload['table']['rows'].append({'c': [{'v': 0}]})

        return payload


processors = [CountStar, DateTrunc]
