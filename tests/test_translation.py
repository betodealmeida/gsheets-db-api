# -*- coding: utf-8 -*-

from .context import gsheetsdb, translate

import unittest

from moz_sql_parser import parse, format


class TranslationTestSuite(unittest.TestCase):

    def test_remove_from(self):
        sql = 'SELECT * FROM table'
        expected = 'SELECT *'
        result = translate(sql, {})
        self.assertEquals(result, expected)

    def test_column_rename(self):
        sql = 'SELECT country, SUM(cnt) FROM "http://example.com" GROUP BY country'
        expected = "SELECT A, SUM(B) GROUP BY A"
        result = translate(sql, {'country': 'A', 'cnt': 'B'})
        self.assertEquals(result, expected)


if __name__ == '__main__':
    unittest.main()
