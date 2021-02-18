from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict

from moz_sql_parser import parse as parse_sql
import pyparsing
import re
from six.moves.urllib import parse


FROM_REGEX = re.compile(' from ("http.*?")', re.IGNORECASE) 


def get_url(url, headers=0, gid=0, sheet=None):

    parts = parse.urlparse(url)

    if parts.path.endswith('/edit'):
        path = parts.path[:-len('/edit')]
    else:
        path = parts.path
    path = '/'.join((path.rstrip('/'), 'gviz/tq'))

    qs = parse.parse_qs(parts.query)

    if 'headers' in qs:
        headers = int(qs['headers'][-1])

    if 'gid' in qs:
        gid = qs['gid'][-1]

    if 'sheet' in qs:
        sheet = qs['sheet'][-1]

    if parts.fragment.startswith('gid='):
        gid = parts.fragment[len('gid='):]

    args = OrderedDict()
    if headers > 0:
        args['headers'] = headers
    if sheet is not None:
        args['sheet'] = sheet
    else:
        args['gid'] = gid
    params = parse.urlencode(args)

    netloc = parts.netloc.replace("\.", ".")

    return parse.urlunparse(
        (parts.scheme, netloc, path, None, params, None))


def extract_url(sql):

    try:
        url = parse_sql(sql)['from']

    except pyparsing.ParseException:
        # fallback to regex to extract from
        match = FROM_REGEX.search(sql)

        if match:
            return match.group(1).strip('"')
        return

    while isinstance(url, dict):
        url = url['value']['from']

    return url


# Function to extract url from any sql statement
def url_from_sql(sql):
    """
    Extract url from any sql statement.
    :param sql:
    :return:
    """

    try:
        parsed_sql = re.split('[( , " )]', str(sql))

        for i, val in enumerate(parsed_sql):
            if val.startswith('https:'):
                sql_url = parsed_sql[i]
                return sql_url

    except Exception as e:
        print("Error: {}".format(e))

