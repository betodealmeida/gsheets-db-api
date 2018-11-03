from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re


POSITION = re.compile(r'at line (?P<line>\d+), column (?P<column>\d+)')


def format_moz_error(query, exception):
    """
    Format syntax error when parsing the original query.

    """
    line = exception.lineno
    column = exception.col
    detailed_message = str(exception)

    msg = query.split('\n')[:line]
    msg.append('{indent}^'.format(indent=' ' * (column - 1)))
    msg.append(detailed_message)
    return '\n'.join(msg)


def format_gsheet_error(query, translated_query, errors):
    """
    Format syntax error returned by API when parsing translated query.

    """
    error_messages = []
    for error in errors:
        detailed_message = error['detailed_message']
        match = POSITION.search(detailed_message)
        if match:
            groups = match.groupdict()
            line = int(groups['line'])
            column = int(groups['column'])

            msg = translated_query.split('\n')[:line]
            msg.append('{indent}^'.format(indent=' ' * (column - 1)))
            msg.append(detailed_message)
            error_messages.append('\n'.join(msg))
        else:
            error_messages.append(detailed_message)

    return """
Original query:
{query}

Translated query:
{translated_query}

Error{plural}:
{error_messages}
    """.format(
        query=query,
        translated_query=translated_query,
        plural='s' if len(errors) > 1 else '',
        error_messages='\n'.join(error_messages),
    ).strip()
