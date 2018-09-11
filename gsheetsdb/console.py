"""Google Spreadsheets CLI

Usage:
  gsheetsdb url [--headers=<headers>] [--gid=<gid>] [--sheet=<sheet>]
  gsheetsdb (-h | --help)
  gsheetsdb --version

Options:
  -h --help             Show this screen.
  --version             Show version.
  --headers=<headers>   Specifies how many rows are header rows
  --gid=<gid>           Specifies which sheet in a multi-sheet document to link
                        to, if you are not linking to the first sheet
  --sheet=<sheet>       Specifies which sheet in a multi-sheet document you are
                        linking to, if you are not linking to the first sheet

"""

from __future__ import unicode_literals

import os
import re
import sys

from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.contrib.completers import WordCompleter
from pygments.lexers import SqlLexer
from pygments.style import Style
from pygments.token import Token
from pygments.styles.default import DefaultStyle
from six.moves.urllib import parse
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


class DocumentStyle(Style):
    styles = {
        Token.Menu.Completions.Completion.Current: 'bg:#00aaaa #000000',
        Token.Menu.Completions.Completion: 'bg:#008888 #ffffff',
        Token.Menu.Completions.ProgressButton: 'bg:#003333',
        Token.Menu.Completions.ProgressBar: 'bg:#00aaaa',
    }
    styles.update(DefaultStyle.styles)


def get_connection_kwargs(url):
    parts = parse.urlparse(url)
    if ':' in parts.netloc:
        host, port = parts.netloc.split(':', 1)
        port = int(port)
    else:
        host = parts.netloc
        port = 8082

    return {
        'host': host,
        'port': port,
        'path': parts.path,
        'scheme': parts.scheme,
    }


def get_autocomplete(connection):
    return keywords + aggregate_functions + scalar_functions


def main(arguments):
    history = FileHistory(os.path.expanduser('~/.gsheetsdb_history'))

    try:
        url = sys.argv[1]
    except IndexError:
        raise Exception('Usage: {0} url'.format(sys.argv[0]))

    kwargs = get_connection_kwargs(url)
    connection = connect(**kwargs)
    cursor = connection.cursor()

    words = get_autocomplete(connection)
    sql_completer = WordCompleter(words, ignore_case=True)

    while True:
        try:
            query = prompt(
                '> ', lexer=SqlLexer, completer=sql_completer,
                style=DocumentStyle, history=history)
        except EOFError:
            break  # Control-D pressed.

        # run query
        query = query.strip('; ')
        if query:
            try:
                result = cursor.execute(query)
            except Exception as e:
                print(e)
                continue

            headers = [t[0] for t in cursor.description or []]
            print(tabulate(result, headers=headers))

    print('GoodBye!')


if __name__ == '__main__':
    from docopt import docopt

    arguments = docopt(__doc__, version=__version__.__version__)
    #main(arguments)
    print(arguments)
