# -*- coding: utf-8 -*-

import unittest

from .context import add_headers, connect, GSheetsDialect


class DialectTestSuite(unittest.TestCase):

    def test_add_headers(self):
        url = 'http://example.com/'
        headers = 10
        result = add_headers(url, headers)
        expected = 'http://example.com/?headers=10&gid=0'
        self.assertEqual(result, expected)

    def test_add_headers_with_gid(self):
        url = 'http://example.com/#gid=10'
        headers = 10
        result = add_headers(url, headers)
        expected = 'http://example.com/?headers=10&gid=10'
        self.assertEqual(result, expected)

    def test_get_pk_constraint(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_pk_constraint(connection, table_name)
        expected = {'constrained_columns': [], 'name': None} 
        self.assertEqual(result, expected)

    def test_get_foreign_keys(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_foreign_keys(connection, table_name)
        expected = []
        self.assertEqual(result, expected)

    def test_get_check_constraints(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_check_constraints(connection, table_name)
        expected = []
        self.assertEqual(result, expected)

    def test_get_table_comment(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_table_comment(connection, table_name)
        expected = {'text': ''}
        self.assertEqual(result, expected)
