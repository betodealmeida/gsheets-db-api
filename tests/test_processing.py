# -*- coding: utf-8 -*-

from collections import OrderedDict
import unittest
import warnings

from moz_sql_parser import parse

from .context import CountStar, is_subset, SubsetMatcher


class ProcessingTestSuite(unittest.TestCase):

    def test_count_star(self):
        sql = 'SELECT COUNT(*) AS total FROM "http://example.com"'
        parsed_query = parse(sql)
        column_map = OrderedDict(sorted({'country': 'A', 'cnt': 'B'}.items()))

        self.assertTrue(CountStar.match(parsed_query))

        processor = CountStar()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = processor.pre_process(parsed_query, column_map)
        expected = parse('''
            SELECT
                COUNT(cnt) AS __CountStar__cnt
              , COUNT(country) AS __CountStar__country
            FROM
                "http://example.com"
        ''')
        self.assertEqual(result, expected)
        self.assertEqual(processor.alias, 'total')

        payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {
                        'id': 'count-A',
                        'label': 'count country',
                        'type': 'number',
                    },
                    {'id': 'count-B', 'label': 'count cnt', 'type': 'number'},
                ],
                'rows': [
                    {
                        'c': [
                            {'v': 9.0},
                            {'v': 8.0},
                        ],
                    },
                ],
            },
        }
        aliases = ['__CountStar__country', '__CountStar__cnt']
        result = processor.post_process(payload, aliases)
        expected = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'count-star', 'label': 'total', 'type': 'number'},
                ],
                'rows': [{'c': [{'v': 9.0}]}],
            },
        }
        self.assertEqual(result, expected)

    def test_count_star_no_results(self):
        sql = 'SELECT COUNT(*) AS total FROM "http://example.com"'
        parsed_query = parse(sql)
        column_map = OrderedDict(sorted({'country': 'A', 'cnt': 'B'}.items()))

        processor = CountStar()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            processor.pre_process(parsed_query, column_map)

        payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {
                        'id': 'count-A',
                        'label': 'count country',
                        'type': 'number',
                    },
                    {'id': 'count-B', 'label': 'count cnt', 'type': 'number'},
                ],
                'rows': [],
            },
        }
        aliases = ['__CountStar__country', '__CountStar__cnt']
        result = processor.post_process(payload, aliases)
        expected = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'count-star', 'label': 'total', 'type': 'number'},
                ],
                'rows': [{'c': [{'v': 0}]}],
            },
        }
        self.assertEqual(result, expected)

    def test_count_star_with_groupby(self):
        sql = (
            'SELECT country, COUNT(*) FROM "http://example.com" '
            'GROUP BY country'
        )
        parsed_query = parse(sql)
        column_map = OrderedDict(sorted({'country': 'A', 'cnt': 'B'}.items()))

        self.assertTrue(CountStar.match(parsed_query))

        processor = CountStar()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = processor.pre_process(parsed_query, column_map)
        expected = parse('''
            SELECT
                country
              , COUNT(cnt) AS __CountStar__cnt
              , COUNT(country) AS __CountStar__country
            FROM
                "http://example.com"
            GROUP BY
                country
        ''')
        self.assertEqual(result, expected)
        self.assertEqual(processor.alias, 'count star')

        payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'count-B',
                        'label': 'count country',
                        'type': 'number',
                    },
                    {'id': 'count-C', 'label': 'count cnt', 'type': 'number'},
                ],
                'rows': [
                    {'c': [{'v': 'BR'}, {'v': 4.0}, {'v': 3.0}]},
                    {'c': [{'v': 'IN'}, {'v': 5.0}, {'v': 1.0}]},
                ],
            },
        }
        aliases = ['country', '__CountStar__country', '__CountStar__cnt']
        result = processor.post_process(payload, aliases)
        expected = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'count-star',
                        'label': 'count star',
                        'type': 'number',
                    },
                ],
                'rows': [
                    {'c': [{'v': 'BR'}, {'v': 4.0}]},
                    {'c': [{'v': 'IN'}, {'v': 5.0}]},
                ],
            },
        }
        self.assertEqual(result, expected)

    def test_subset_matcher(self):
        pattern = SubsetMatcher({'select': {'value': {'count': '*'}}})

        parsed_query = parse('SELECT COUNT(*)')
        self.assertTrue(pattern.match(parsed_query))

        parsed_query = parse('SELECT COUNT(*) AS total')
        self.assertTrue(pattern.match(parsed_query))

        parsed_query = parse('SELECT COUNT(*) AS total, country')
        self.assertTrue(pattern.match(parsed_query))

        parsed_query = parse('SELECT cnt, COUNT(*) AS total, country')
        self.assertTrue(pattern.match(parsed_query))

        parsed_query = parse('SELECT country')
        self.assertFalse(pattern.match(parsed_query))

        parsed_query = parse(
            'SELECT country, COUNT(*) FROM "http://example.com" '
            'GROUP BY country')
        self.assertTrue(pattern.match(parsed_query))

    def test_is_subset(self):
        json = [1, 2, 3]

        other = [1, 2, 3, 4]
        self.assertTrue(is_subset(json, other))

        other = 1
        self.assertFalse(is_subset(json, other))

        other = [1, 3, 4]
        self.assertFalse(is_subset(json, other))
