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

    # TODO: review these
    # http://docs.sqlalchemy.org/en/latest/core/internals.html#sqlalchemy.engine.interfaces.Dialect
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
        self.catalog = "https://docs.google.com/spreadsheets/d/1423FwKsIWozWDqZmgn52DughAOSsz-KCcAy3lBM3pIM/edit#gid=0"
        print(url)
        return ([], {})

    def get_schema_names(self, connection, **kwargs):
        return ['default']

    def has_table(self, connection, table_name, schema=None):
        return True

    def get_table_names(self, connection, schema=None, **kwargs):
        return [
            'https://docs.google.com/spreadsheets/d/1423FwKsIWozWDqZmgn52DughAOSsz-KCcAy3lBM3pIM/edit?headers=1#gid=0',
            'https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit?headers=2#gid=1077884006',
        ]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = "SELECT * FROM {table_name} LIMIT 0"
        result = connection.execute(query)

        return [
            {
                'name': col[0],
                'type': col[1],
                'nullable': True,
                'default': None,
            }
            for col in result.description
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
