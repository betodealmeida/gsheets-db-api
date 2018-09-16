# -*- coding: utf-8 -*-

from .context import exceptions, gsheetsdb, translate

import unittest

from moz_sql_parser import parse, format


class TranslationTestSuite(unittest.TestCase):

    def test_select(self):
        sql = 'SELECT country FROM "http://example.com"'
        expected = "SELECT A"
        result = translate(sql, {'country': 'A'})
        self.assertEquals(result, expected)

    def test_from(self):
        sql = 'SELECT * FROM table'
        expected = 'SELECT *'
        result = translate(sql, {})
        self.assertEquals(result, expected)

    def test_where(self):
        sql = 'SELECT country FROM "http://example.com" WHERE cnt > 10'
        expected = 'SELECT A WHERE B > 10'
        result = translate(sql, {'country': 'A', 'cnt': 'B'})
        self.assertEquals(result, expected)

    def test_where_groupby(self):
        sql = '''SELECT country, SUM(cnt) FROM "http://example.com" WHERE country != 'US' GROUP BY country'''
        expected = "SELECT A, SUM(B) WHERE A <> 'US' GROUP BY A"
        result = translate(sql, {'country': 'A', 'cnt': 'B'})
        self.assertEquals(result, expected)

    def test_groupby(self):
        sql = 'SELECT country, SUM(cnt) FROM "http://example.com" GROUP BY country'
        expected = "SELECT A, SUM(B) GROUP BY A"
        result = translate(sql, {'country': 'A', 'cnt': 'B'})
        self.assertEquals(result, expected)

    def test_having(self):
        sql = '''
    SELECT
        country
      , SUM(cnt)
    FROM
        "http://example.com"
    GROUP BY
        country
    HAVING
        COUNT(*) > 0
        '''
        with self.assertRaises(exceptions.NotSupportedError):
            result = translate(sql, {'country': 'A', 'cnt': 'B'})

    def test_orderby(self):
        sql = '''
    SELECT
        country
      , SUM(cnt)
    FROM
        "http://example.com"
    GROUP BY
        country
    ORDER BY
        SUM(cnt)
        '''
        expected = "SELECT A, SUM(B) GROUP BY A ORDER BY SUM(B)"
        result = translate(sql, {'country': 'A', 'cnt': 'B'})
        self.assertEquals(result, expected)

    def test_limit(self):
        sql = 'SELECT country FROM "http://example.com" LIMIT 10'
        expected = 'SELECT A LIMIT 10'
        result = translate(sql, {'country': 'A'})
        self.assertEquals(result, expected)

    def test_where(self):
        sql = 'SELECT country FROM "http://example.com" LIMIT 10 OFFSET 5'
        expected = 'SELECT A LIMIT 10 OFFSET 5'
        result = translate(sql, {'country': 'A'})
        self.assertEquals(result, expected)


if __name__ == '__main__':
    unittest.main()
