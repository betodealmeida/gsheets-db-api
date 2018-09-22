# -*- coding: utf-8 -*-

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock

import unittest

import requests_mock
from sqlalchemy import MetaData, select, Table
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import sqltypes

from .context import add_headers, connect, gsheetsdb, GSheetsDialect, Type


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

    def test_get_schema_names(self):
        connection = Mock()
        connection.execute = Mock()
        result = Mock()
        result.fetchall = Mock()
        result.fetchall.return_value = [('default', 2), ('public', 4)]
        connection.execute.return_value = result

        dialect = GSheetsDialect()
        url = make_url('gsheets://example.com/')
        dialect.create_connect_args(url)
        result = dialect.get_schema_names(connection)
        expected = ['default', 'public']
        self.assertEqual(result, expected)

    def test_get_schema_names_no_catalog(self):
        connection = connect()
        dialect = GSheetsDialect()
        url = make_url('gsheets://')
        dialect.create_connect_args(url)
        result = dialect.get_schema_names(connection)
        expected = []
        self.assertEqual(result, expected)

    def test_get_table_names(self):
        connection = Mock()
        connection.execute = Mock()
        result = Mock()
        result.fetchall = Mock()
        result.fetchall.return_value = [
            ('http://example.com/edit#gid=0', 2),
            ('http://example.com/edit#gid=1', 1),
        ]
        connection.execute.return_value = result

        dialect = GSheetsDialect()
        url = make_url('gsheets://example.com/')
        dialect.create_connect_args(url)
        result = dialect.get_table_names(connection)
        expected = [
            'http://example.com/edit?headers=2&gid=0',
            'http://example.com/edit?headers=1&gid=1',
        ]
        self.assertEqual(result, expected)

    def test_get_table_names_no_catalog(self):
        connection = connect()
        dialect = GSheetsDialect()
        url = make_url('gsheets://')
        dialect.create_connect_args(url)
        result = dialect.get_table_names(connection)
        expected = []
        self.assertEqual(result, expected)

    def test_has_table(self):
        connection = Mock()
        connection.execute = Mock()
        result = Mock()
        result.fetchall = Mock()
        result.fetchall.return_value = [
            ('http://example.com/edit#gid=0', 2),
            ('http://example.com/edit#gid=1', 1),
        ]
        connection.execute.return_value = result

        dialect = GSheetsDialect()
        url = make_url('gsheets://example.com/')
        dialect.create_connect_args(url)

        self.assertTrue(
            dialect.has_table(
                connection, 'http://example.com/edit?headers=2&gid=0'))
        self.assertFalse(
            dialect.has_table(
                connection, 'http://example.com/edit?headers=2&gid=1'))

    def test_has_table_no_catalog(self):
        connection = connect()
        dialect = GSheetsDialect()
        url = make_url('gsheets://')
        dialect.create_connect_args(url)
        self.assertTrue(dialect.has_table(connection, 'ANY TABLE'))

    def test_get_columns(self):
        description = [
            ('datetime', Type.DATETIME, None, None, None, None, True),
            ('number', Type.NUMBER, None, None, None, None, True),
            ('boolean', Type.BOOLEAN, None, None, None, None, True),
            ('date', Type.DATE, None, None, None, None, True),
            ('timeofday', Type.TIMEOFDAY, None, None, None, None, True),
            ('string', Type.STRING, None, None, None, None, True),
        ]
        connection = Mock()
        connection.execute = Mock()
        result = Mock()
        result._cursor_description = Mock()
        result._cursor_description.return_value = description
        connection.execute.return_value = result

        dialect = GSheetsDialect()
        url = make_url('gsheets://example.com/')
        dialect.create_connect_args(url)

        result = dialect.get_columns(connection, 'SOME TABLE')
        expected = [
            {
                'name': 'datetime',
                'type': sqltypes.DATETIME,
                'nullable': True,
                'default': None,
            },
            {
                'name': 'number',
                'type': sqltypes.Numeric,
                'nullable': True,
                'default': None,
            },
            {
                'name': 'boolean',
                'type': sqltypes.Boolean,
                'nullable': True,
                'default': None,
            },
            {
                'name': 'date',
                'type': sqltypes.DATE,
                'nullable': True,
                'default': None,
            },
            {
                'name': 'timeofday',
                'type': sqltypes.TIME,
                'nullable': True,
                'default': None,
            },
            {
                'name': 'string',
                'type': sqltypes.String,
                'nullable': True,
                'default': None,
            },
        ]
        self.assertEqual(result, expected)

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

    @requests_mock.Mocker()
    def test_GSheetsCompiler(self, m):
        header_payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'B',
                        'label': 'cnt',
                        'type': 'number',
                        'pattern': 'General',
                    },
                ],
                'rows': [],
            },
        }
        m.get(
            'http://example.com/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200',
            json=header_payload,
        )
        engine = create_engine('gsheets://')
        table = Table(
            'http://example.com/', MetaData(bind=engine), autoload=True)
        query = select([table.columns.country], from_obj=table)
        result = str(query)
        expected = 'SELECT country \nFROM "http://example.com/"'
        self.assertEqual(result, expected)
