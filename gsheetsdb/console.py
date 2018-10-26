"""Google Spreadsheets CLI

Usage:
  gsheetsdb [--headers=<headers>] [--raise]
  gsheetsdb (-h | --help)
  gsheetsdb --version

Options:
  -h --help             Show this screen.
  --version             Show version.
  --headers=<headers>   Specifies how many rows are header rows [default: 0]

"""

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
    connection = connect()
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
