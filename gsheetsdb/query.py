from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict
import json

from google.auth.transport.requests import AuthorizedSession
from moz_sql_parser import parse as parse_sql
import pyparsing
from requests import Session
from six.moves.urllib import parse

from gsheetsdb.convert import convert_rows
from gsheetsdb.exceptions import ProgrammingError
from gsheetsdb.processors import processors
from gsheetsdb.translator import extract_column_aliases, translate
from gsheetsdb.types import Type
from gsheetsdb.url import extract_url, get_url
from gsheetsdb.utils import format_gsheet_error, format_moz_error


# the JSON payload has this in the beginning
LEADING = ")]}'\n"


def get_column_map(url, credentials=None):
    query = 'SELECT * LIMIT 0'
    result = run_query(url, query, credentials)
    return OrderedDict(
        sorted((col['label'], col['id']) for col in result['table']['cols']))


def run_query(baseurl, query, credentials=None):
    url = '{baseurl}&tq={query}'.format(
        baseurl=baseurl, query=parse.quote(query, safe='/()'))
    headers = {'X-DataSource-Auth': 'true'}

    if credentials:
        session = AuthorizedSession(credentials)
    else:
        session = Session()

    r = session.get(url, headers=headers)
    if r.encoding is None:
        r.encoding = 'utf-8'

    # raise any error messages
    if r.status_code != 200:
        raise ProgrammingError(r.text)

    if r.text.startswith(LEADING):
        result = json.loads(r.text[len(LEADING):])
    else:
        result = r.json()

    return result


def get_description_from_payload(payload):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            col['label'],               # name
            Type[col['type'].upper()],  # type_code
            None,                       # [display_size]
            None,                       # [internal_size]
            None,                       # [precision]
            None,                       # [scale]
            True,                       # [null_ok]
        )
        for col in payload['table']['cols']
    ]


def execute(query, headers=0, credentials=None):
    try:
        parsed_query = parse_sql(query)
    except pyparsing.ParseException as e:
        raise ProgrammingError(format_moz_error(query, e))

    # fetch aliases, since they will be removed by the translator
    original_aliases = extract_column_aliases(parsed_query)

    # extract URL from the `FROM` clause
    from_ = extract_url(query)
    baseurl = get_url(from_, headers)

    # map between labels and ids, eg, `{ 'country': 'A' }`
    column_map = get_column_map(baseurl, credentials)

    # preprocess
    used_processors = []
    for cls in processors:
        if cls.match(parsed_query):
            processor = cls()
            parsed_query = processor.pre_process(parsed_query, column_map)
            used_processors.append(processor)
    processed_aliases = extract_column_aliases(parsed_query)

    # translate colum names to ids and remove aliases
    translated_query = translate(parsed_query, column_map)

    # run query
    payload = run_query(baseurl, translated_query, credentials)
    if payload['status'] == 'error':
        raise ProgrammingError(
            format_gsheet_error(query, translated_query, payload['errors']))

    # postprocess
    for processor in used_processors:
        payload = processor.post_process(payload, processed_aliases)

    # add aliases back
    cols = payload['table']['cols']
    for alias, col in zip(original_aliases, cols):
        if alias is not None:
            col['label'] = alias

    description = get_description_from_payload(payload)

    # convert rows to proper type (datetime, eg)
    rows = payload['table']['rows']
    results = convert_rows(cols, rows)

    return results, description
