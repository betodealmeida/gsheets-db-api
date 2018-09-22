# -*- coding: utf-8 -*-

import unittest

from sqlalchemy.engine.url import make_url

from .context import add_headers, connect, gsheetsdb, GSheetsDialect


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

    def test_cls_dbapi(self):
        self.assertEqual(GSheetsDialect.dbapi(), gsheetsdb)

    def test_create_connect_args(self):
        dialect = GSheetsDialect()

        url = make_url('gsheets://')
        args = dialect.create_connect_args(url)
        self.assertEqual(args, ([], {}))
        self.assertIsNone(dialect.url)

        url = make_url('gsheets://example.com/')
        args = dialect.create_connect_args(url)
        self.assertEqual(args, ([], {}))
        self.assertEqual(
            dialect.url, '{0}://example.com/'.format(dialect.scheme))

    def test_get_view_names(self):
        connection = connect()
        dialect = GSheetsDialect()
        result = dialect.get_view_names(connection)
        expected = []
        self.assertEqual(result, expected)

    def test_get_table_options(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_table_options(connection, table_name)
        expected = {}
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

    def test_get_indexes(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_indexes(connection, table_name)
        expected = []
        self.assertEqual(result, expected)

    def test_get_unique_constraints(self):
        connection = connect()
        table_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_unique_constraints(connection, table_name)
        expected = []
        self.assertEqual(result, expected)

    def test_get_view_definition(self):
        connection = connect()
        view_name = 'http://example.com/'
        dialect = GSheetsDialect()
        result = dialect.get_view_definition(connection, view_name)
        self.assertIsNone(result)

    def test_do_rollback(self):
        connection = connect()
        dialect = GSheetsDialect()
        result = dialect.do_rollback(connection)
        self.assertIsNone(result)

    def test__check_unicode_returns(self):
        connection = connect()
        dialect = GSheetsDialect()
        result = dialect._check_unicode_returns(connection)
        self.assertTrue(result)

    def test__check_unicode_description(self):
        connection = connect()
        dialect = GSheetsDialect()
        result = dialect._check_unicode_description(connection)
        self.assertTrue(result)
