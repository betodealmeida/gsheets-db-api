from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict

from six.moves.urllib import parse
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import types

import gsheetsdb
from gsheetsdb.auth import get_credentials_from_auth


type_map = {
    'string': types.String,
    'number': types.Numeric,
    'boolean': types.Boolean,
    'date': types.DATE,
    'datetime': types.DATETIME,
    'timeofday': types.TIME,
}


def add_headers(url, headers):
    parts = parse.urlparse(url)
    if parts.fragment.startswith('gid='):
        gid = parts.fragment[len('gid='):]
    else:
        gid = 0
    params = parse.urlencode(OrderedDict([('headers', headers), ('gid', gid)]))
    return parse.urlunparse(
        (parts.scheme, parts.netloc, parts.path, None, params, None))


class GSheetsIdentifierPreparer(compiler.IdentifierPreparer):
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


class GSheetsCompiler(compiler.SQLCompiler):
    def visit_column(self, column, **kwargs):
        if column.table is not None:
            column.table.named_with_column = False
        return super(GSheetsCompiler, self).visit_column(column, **kwargs)

    def visit_table(
        self,
        table,
        asfrom=False,
        iscrud=False,
        ashint=False,
        fromhints=None,
        use_schema=False,
        **kwargs
    ):
        return super(GSheetsCompiler, self).visit_table(
            table, asfrom, iscrud, ashint, fromhints, False, **kwargs)


class GSheetsTypeCompiler(compiler.GenericTypeCompiler):
    pass


class GSheetsDialect(default.DefaultDialect):

    # TODO: review these
    # http://docs.sqlalchemy.org/en/latest/core/internals.html#sqlalchemy.engine.interfaces.Dialect
    name = 'gsheets'
    scheme = 'https'
    driver = 'rest'
    preparer = GSheetsIdentifierPreparer
    statement_compiler = GSheetsCompiler
    type_compiler = GSheetsTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(
        self,
        service_account_file=None,
        service_account_info=None,
        subject=None,
        *args,
        **kwargs
    ):
        super(GSheetsDialect, self).__init__(*args, **kwargs)
        self.credentials = get_credentials_from_auth(
            service_account_file, service_account_info, subject)

    @classmethod
    def dbapi(cls):
        return gsheetsdb

    def create_connect_args(self, url):
        port = ':{url.port}'.format(url=url) if url.port else ''
        if url.host is None:
            self.url = None
        else:
            self.url = '{scheme}://{host}{port}/{database}'.format(
                scheme=self.scheme,
                host=url.host,
                port=port,
                database=url.database or '',
            )
        return ([self.credentials], {})

    def get_schema_names(self, connection, **kwargs):
        if self.url is None:
            return []

        query = 'SELECT C, COUNT(C) FROM "{catalog}" GROUP BY C'.format(
            catalog=self.url)
        result = connection.execute(query)
        return [row[0] for row in result.fetchall()]

    def has_table(self, connection, table_name, schema=None):
        if self.url is None:
            return True

        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs):
        if self.url is None:
            return []

        query = 'SELECT * FROM "{catalog}"'.format(catalog=self.url)
        if schema:
            query = "{query} WHERE C='{schema}'".format(
                query=query, schema=schema)
        result = connection.execute(query)
        return [add_headers(row[0], int(row[1])) for row in result.fetchall()]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = 'SELECT * FROM "{table}" LIMIT 0'.format(table=table_name)
        result = connection.execute(query)
        return [
            {
                'name': col[0],
                'type': type_map[col[1].value],
                'nullable': True,
                'default': None,
            }
            for col in result._cursor_description()
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
