# -*- coding: utf-8 -*-

import unittest

from moz_sql_parser import parse

from .context import exceptions, extract_column_aliases, translate


class TranslationTestSuite(unittest.TestCase):

    def test_select(self):
        sql = 'SELECT country FROM "http://docs.google.com"'
        expected = "SELECT A"
        result = translate(parse(sql), {'country': 'A'})
        self.assertEqual(result, expected)

    def test_from(self):
        sql = 'SELECT * FROM table'
        expected = 'SELECT *'
        result = translate(parse(sql), {})
        self.assertEqual(result, expected)

    def test_where(self):
        sql = 'SELECT country FROM "http://docs.google.com" WHERE cnt > 10'
        expected = 'SELECT A WHERE B > 10'
        result = translate(parse(sql), {'country': 'A', 'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_where_groupby(self):
        sql = '''
            SELECT
                country
              , SUM(cnt)
            FROM
                "http://docs.google.com"
            WHERE
                country != 'US'
            GROUP BY
                country
        '''
        expected = "SELECT A, SUM(B) WHERE A <> 'US' GROUP BY A"
        result = translate(parse(sql), {'country': 'A', 'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_groupby(self):
        sql = (
            'SELECT country, SUM(cnt) FROM "http://docs.google.com" '
            'GROUP BY country'
        )
        expected = "SELECT A, SUM(B) GROUP BY A"
        result = translate(parse(sql), {'country': 'A', 'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_having(self):
        sql = '''
    SELECT
        country
      , SUM(cnt)
    FROM
        "http://docs.google.com"
    GROUP BY
        country
    HAVING
        COUNT(*) > 0
        '''
        with self.assertRaises(exceptions.NotSupportedError):
            translate(parse(sql), {'country': 'A', 'cnt': 'B'})

    def test_subquery(self):
        sql = 'SELECT * from XYZZY, ABC'
        with self.assertRaises(exceptions.NotSupportedError):
            translate(parse(sql))

    def test_orderby(self):
        sql = '''
    SELECT
        country
      , SUM(cnt)
    FROM
        "http://docs.google.com"
    GROUP BY
        country
    ORDER BY
        SUM(cnt)
        '''
        expected = "SELECT A, SUM(B) GROUP BY A ORDER BY SUM(B)"
        result = translate(parse(sql), {'country': 'A', 'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_limit(self):
        sql = 'SELECT country FROM "http://docs.google.com" LIMIT 10'
        expected = 'SELECT A LIMIT 10'
        result = translate(parse(sql), {'country': 'A'})
        self.assertEqual(result, expected)

    def test_offset(self):
        sql = 'SELECT country FROM "http://docs.google.com" LIMIT 10 OFFSET 5'
        expected = 'SELECT A LIMIT 10 OFFSET 5'
        result = translate(parse(sql), {'country': 'A'})
        self.assertEqual(result, expected)

    def test_alias(self):
        sql = 'SELECT SUM(cnt) AS total FROM "http://docs.google.com"'
        expected = 'SELECT SUM(B)'
        result = translate(parse(sql), {'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_multiple_aliases(self):
        sql = (
            'SELECT country AS dim1, SUM(cnt) AS total '
            'FROM "http://docs.google.com" GROUP BY country'
        )
        expected = 'SELECT A, SUM(B) GROUP BY A'
        result = translate(parse(sql), {'country': 'A', 'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_unalias_orderby(self):
        sql = '''
        SELECT
            cnt AS value
        FROM
            "http://docs.google.com"
        ORDER BY
            value
        '''
        expected = 'SELECT B ORDER BY B'
        result = translate(parse(sql), {'cnt': 'B'})
        self.assertEqual(result, expected)

    def test_column_aliases(self):
        sql = 'SELECT SUM(cnt) AS total FROM "http://docs.google.com"'
        expected = ['total']
        result = extract_column_aliases(parse(sql))
        self.assertEqual(result, expected)

    def test_column_aliases_star(self):
        sql = 'SELECT * FROM "http://docs.google.com"'
        expected = [None]
        result = extract_column_aliases(parse(sql))
        self.assertEqual(result, expected)

    def test_column_aliases_multiple(self):
        sql = (
            'SELECT SUM(cnt) AS total, country, gender AS dim1 '
            'FROM "http://docs.google.com"'
        )
        expected = ['total', None, 'dim1']
        result = extract_column_aliases(parse(sql))
        self.assertEqual(result, expected)

    def test_order_by_alias(self):
        sql = '''
    SELECT
        country AS country
      , SUM(cnt) AS "SUM(cnt)"
    FROM
        "https://docs.google.com"
    GROUP BY
        country
    ORDER BY
        "SUM(cnt)"
    DESC
         LIMIT 10
        '''
        expected = 'SELECT A, SUM(B) GROUP BY A ORDER BY SUM(B) DESC LIMIT 10'
        result = translate(parse(sql), {'country': 'A', 'cnt': 'B'})
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
