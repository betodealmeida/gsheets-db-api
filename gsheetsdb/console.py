"""Google Spreadsheets CLI

Usage:
  gsheetsdb [--headers=<headers>] [--raise] [--service-account-file=<file> [--subject=<subject>]]
  gsheetsdb (-h | --help)
  gsheetsdb --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --headers=<headers>               How many rows are headers [default: 0]
  --service-account-file=<file>     Service account file for authentication
  --subject=<subject>               Subject to impersonate

"""  # noqa: E501

from __future__ import unicode_literals

import os

from docopt import docopt
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.lexers import SqlLexer
from pygments.styles import get_style_by_name
from tabulate import tabulate

from gsheetsdb import connect, __version__
from gsheetsdb.auth import get_credentials_from_auth


keywords = [
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
]

aggregate_functions = [
    'avg',
    'count',
    'max',
    'min',
    'sum',
]

scalar_functions = [
    'year',
    'month',
    'day',
    'hour',
    'minute',
    'second',
    'millisecond',
    'quarter',
    'dayOfWeek',
    'now',
    'dateDiff',
    'toDate',
    'upper',
    'lower',
]


def main():
    history = FileHistory(os.path.expanduser('~/.gsheetsdb_history'))

    arguments = docopt(__doc__, version=__version__.__version__)

    auth = {
        'service_account_file': arguments['--service-account-file'],
        'subject': arguments['--subject'],
    }
    credentials = get_credentials_from_auth(**auth)
    connection = connect(credentials)
    headers = int(arguments['--headers'])
    cursor = connection.cursor()

    lexer = PygmentsLexer(SqlLexer)
    words = keywords + aggregate_functions + scalar_functions
    completer = WordCompleter(words, ignore_case=True)
    style = style_from_pygments_cls(get_style_by_name('manni'))

    while True:
        try:
            query = prompt(
                'sql> ', lexer=lexer, completer=completer,
                style=style, history=history)
        except (EOFError, KeyboardInterrupt):
            break  # Control-D pressed.

        # run query
        query = query.strip('; ').replace('%', '%%')
        if query:
            try:
                result = cursor.execute(query, headers=headers)
            except Exception as e:
                if arguments['--raise']:
                    raise
                print(e)
                continue

            columns = [t[0] for t in cursor.description or []]
            print(tabulate(result, headers=columns))

    print('See ya!')
