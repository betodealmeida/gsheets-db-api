from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict

from moz_sql_parser import parse as parse_sql
from six.moves.urllib import parse


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

    return parse.urlunparse(
        (parts.scheme, parts.netloc, path, None, params, None))


def extract_url(sql):
    parsed_query = parse_sql(sql)
    return parsed_query['from']
