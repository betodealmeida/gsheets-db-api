# -*- coding: future_fstrings -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import types

import gsheetsdb
from gsheetsdb import exceptions


type_map = {
    'string': types.String,
    'number': types.Numeric,
    'boolean': types.Boolean,
    'date': types.DATE,
    'datetime': types.DATETIME,
    'timeofday': types.TIME,
}


class GsheetsIdentifierPreparer(compiler.IdentifierPreparer):
    # https://developers.google.com/chart/interactive/docs/querylanguage#reserved-words
    reserved_words = {
        'and',
        'asc',
        'by',
        'date',
        'datetime',
        'desc',
        'false',
        'format',
        'group',
        'label',
        'limit',
        'not',
        'offset',
        'options',
        'or',
        'order',
        'pivot',
        'select',
        'timeofday',
        'timestamp',
        'true',
        'where',
    }


class GsheetsCompiler(compiler.SQLCompiler):
    pass


class GsheetsTypeCompiler(compiler.GenericTypeCompiler):
    pass


class GsheetsDialect(default.DefaultDialect):

    name = 'gsheets'
    driver = 'rest'
    preparer = GsheetsIdentifierPreparer
    statement_compiler = GsheetsCompiler
    type_compiler = GsheetsTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        return gsheetsdb

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 8082,
            'path': url.database,
            'scheme': self.scheme,
        }
        return ([], kwargs)

    def get_schema_names(self, connection, **kwargs):
        # Each Gsheets datasource appears as a table in the "gsheets" schema. This
        # is also the default schema, so Gsheets datasources can be referenced as
        # either gsheets.dataSourceName or simply dataSourceName.
        result = connection.execute(
            'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA')

        return [row.SCHEMA_NAME for row in result]

    def has_table(self, connection, table_name, schema=None):
        query = f"""
            SELECT COUNT(*) > 0 AS exists_
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = '{table_name}'
        """

        result = connection.execute(query)
        return result.fetchone().exists_

    def get_table_names(self, connection, schema=None, **kwargs):
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
        if schema:
            query = f"{query} WHERE TABLE_SCHEMA = '{schema}'"

        result = connection.execute(query)
        return [row.TABLE_NAME for row in result]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = f"""
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   COLUMN_DEFAULT
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = '{table_name}'
        """
        if schema:
            query = f"{query} AND TABLE_SCHEMA = '{schema}'"

        result = connection.execute(query)

        return [
            {
                'name': row.COLUMN_NAME,
                'type': type_map[row.DATA_TYPE.lower()],
                'nullable': get_is_nullable(row.IS_NULLABLE),
                'default': get_default(row.COLUMN_DEFAULT),
            }
            for row in result
        ]

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kwargs
    ):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {'text': ''}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kwargs
    ):
        return []

    def get_view_definition(
        self,
        connection,
        view_name,
        schema=None,
        **kwargs
    ):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True


def get_is_nullable(gsheets_is_nullable):
    # this should be 'YES' or 'NO'; we default to no
    return gsheets_is_nullable.lower() == 'yes'


def get_default(gsheets_column_default):
    # currently unused, returns ''
    return str(gsheets_column_default) if gsheets_column_default != '' else None
