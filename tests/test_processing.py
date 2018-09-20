# -*- coding: utf-8 -*-

from .context import CountStar, SubsetMatcher

import unittest

from moz_sql_parser import format, parse


class ProcessingTestSuite(unittest.TestCase):

    def test_count_star(self):
        sql = 'SELECT COUNT(*) AS total FROM "http://example.com"'
        parsed_query = parse(sql)
        column_map = {'country': 'A', 'cnt': 'B'}

        self.assertTrue(CountStar.match(parsed_query))

        processor = CountStar()
        result = processor.pre_process(parsed_query, column_map)
        expected = parse('''
            SELECT
                COUNT(country) AS __CountStar__country
              , COUNT(cnt) AS __CountStar__cnt
            FROM
                "http://example.com"
        ''')
        self.assertEquals(result, expected)
        self.assertEquals(processor.alias, 'total')

        payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'count-A', 'label': 'count country', 'type': 'number'},
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
        self.assertEquals(result, expected)

    def test_count_star_with_groupby(self):
        sql = 'SELECT country, COUNT(*) FROM "http://example.com" GROUP BY country'
        parsed_query = parse(sql)
        column_map = {'country': 'A', 'cnt': 'B'}

        self.assertTrue(CountStar.match(parsed_query))

        processor = CountStar()

        result = processor.pre_process(parsed_query, column_map)
        expected = parse('''
            SELECT
                country
              , COUNT(country) AS __CountStar__country
              , COUNT(cnt) AS __CountStar__cnt
            FROM
                "http://example.com"
            GROUP BY
                country
        ''')
        self.assertEquals(result, expected)
        self.assertEquals(processor.alias, 'count star')

        payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {'id': 'count-B', 'label': 'count country', 'type': 'number'},
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
                    {'id': 'count-star', 'label': 'count star', 'type': 'number'},
                ],
                'rows': [
                    {'c': [{'v': 'BR'}, {'v': 4.0}]},
                    {'c': [{'v': 'IN'}, {'v': 5.0}]},
                ],
            },
        }
        self.assertEquals(result, expected)

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
            'SELECT country, COUNT(*) FROM "http://example.com" GROUP BY country')
        self.assertTrue(pattern.match(parsed_query))
