from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime
import logging
import sqlite3

from gsheetsdb.convert import convert_rows
from gsheetsdb.exceptions import ProgrammingError
from gsheetsdb.query import run_query
from gsheetsdb.url import extract_url, get_url

import time

# import libraries by (JG)
import sqlparse
import re
from googleapiclient import discovery
from pprint import pprint
from gsheetsdb.url import url_from_sql
from six.moves.urllib import parse


logger = logging.getLogger(__name__)

# Google Spreadsheet types to SQLite types
typemap = {
    'string': 'text',
    'number': 'real',
    'boolean': 'boolean',
    'date': 'date',
    'datetime': 'timestamp',
    'timeofday': 'timeofday',
}


def adapt_timeofday(timeofday):
    return (
        3600e6 * timeofday.hour +
        60e6 * timeofday.minute +
        1e6 * timeofday.second +
        timeofday.microsecond
    )


def convert_timeofday(val):
    val = int(val)
    hour, val = divmod(val, int(3600e6))
    minute, val = divmod(val, int(60e6))
    second, val = divmod(val, int(1e6))
    microsecond = int(val)
    return datetime.time(hour, minute, second, microsecond)


sqlite3.register_adapter(datetime.time, adapt_timeofday)
sqlite3.register_converter('timeofday', convert_timeofday)


def create_table(cursor, table, payload):

    cols = ', '.join(
        '"{name}" {type}'.format(
            name=col['label'] or col['id'], type=typemap[col['type']])
        for col in payload['table']['cols']
    )

    query = 'CREATE TABLE "{table}" ({cols})'.format(table=table, cols=cols)
    logger.info(query)
    cursor.execute(query)


def insert_into(cursor, table, payload):
    cols = payload['table']['cols']

    values = ', '.join('?' for col in cols)
    query = 'INSERT INTO "{table}" VALUES ({values})'.format(
        table=table, values=values)
    rows = convert_rows(cols, payload['table']['rows'])

    logger.info(query)
    cursor.executemany(query, rows)


def execute(query, headers=0, credentials=None):

    # fetch all the data
    # Added by (JG)
    # Parse query to extract url
    parsed = sqlparse.parse(query)[0]
    parsed_token = parsed.tokens

    if str(parsed_token[0]) == 'INSERT':

        from_ = parsed_token[4]   # sheet url

    else:
        from_ = extract_url(query)

    if not from_:
        raise ProgrammingError('Invalid query: {query}'.format(query=query))

    baseurl = get_url(from_, headers)
    payload = run_query(baseurl, 'SELECT *', credentials)

    # create table
    conn = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    create_table(cursor, from_, payload)
    insert_into(cursor, from_, payload)

    conn.commit()

    # run query in SQLite instead
    logger.info('SQLite query: {}'.format(query))

    results = cursor.execute(query).fetchall()
    description = cursor.description

    return results, description


# Function to execute sql query other than 'SELECT'
def execute_all_sql(query, headers=0, credentials=None):
    """
    Execute INSERT, UPDATE, DELETE
    :param query:
    :param headers:
    :param credentials:
    :return:
    """

    # fetch all data
    # get url
    from_ = url_from_sql(query)

    if not from_:
        raise ProgrammingError('Invalid query: {query}'.format(query=query))

    baseurl = get_url(from_, headers)
    payload = run_query(baseurl, 'SELECT *', credentials)

    # create table
    conn = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    create_table(cursor, from_, payload)
    insert_into(cursor, from_, payload)

    # run query in SQLite memory
    logger.info('SQLite query: {}'.format(query))
    cursor.execute(query)

    # Fetch column description and data from sqlite after modification
    stmt = get_col_names(cursor, from_)
    cols = tuple(stmt)

    records = from_sqlite(cursor, from_)
    print("1000.1 records: {}".format(records))

    records.insert(0, cols)

    # Append records to google spreadsheet
    # retrieve sheet_id and sheet name from url
    sheet_meta = get_sheet_meta(credentials, from_)
    print("meta: {}".format(sheet_meta))
    sheet_id = sheet_meta[0]
    sheet_name = sheet_meta[1]

    # If query is 'INSERT', append new data in the same worksheet
    parsed = sqlparse.parse(query)[0]
    tok = parsed.tokens
    first_word = str(tok[0])

    if first_word == 'INSERT':
        write_gsheet(credentials, sheet_id, sheet_name, records)

    # If not 'INSERT', upload updated record in a new worksheet
    else:
        write_new_worksheet(credentials, sheet_id, sheet_name, records)

    conn.commit()

    return


def from_sqlite(cursor, table):
    """
    Retrieve data from SQLite memory.
    :param cursor:
    :param table:
    :return:
    """

    record = []
    query = 'SELECT * FROM "{table}"'.format(table=table)
    logger.info(query)
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        record.append(row)

    return record


def write_gsheet(creds, sheet_id, sheet_name, values):
    """
    Append data in same worksheet
    :param creds:
    :param sheet_id:
    :param sheet_name:
    :param values:
    :return:
    """
    try:
        value_range_body = {
                "majorDimension": "ROWS",
                'values': values
            }

        # Call the Sheets API
        service = discovery.build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # upload data into google spreadsheet
        # upload in the same worksheet
        request = sheet.values().update(spreadsheetId=sheet_id, valueInputOption="USER_ENTERED",
                                        range=sheet_name + '!A1', body=value_range_body)

        response = request.execute()

        print("Updating done successfully!")
        pprint(response)

    except Exception as e:
        print("Error: {}".format(e))


def write_new_worksheet(creds, sheet_id, sheet_name, values):
    """
    Append data into new worksheet.
    :param creds:
    :param sheet_id:
    :param sheet_name:
    :param values:
    :return:
    """

    try:
        value_range_body = {
                "majorDimension": "ROWS",
                'values': values
            }

        # Call the Sheets API
        service = discovery.build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # todo: Upload in a new worksheet
        # Add a new sheet and upload data into the new sheet
        new_sheet = "{}_1".format(sheet_name)
        add_sheets(creds, sheet_id, new_sheet)

        request = sheet.values().update(spreadsheetId=sheet_id, valueInputOption="USER_ENTERED",
                                        range=new_sheet + '!A1', body=value_range_body)

        response = request.execute()

        print("Updated in a new sheet {} doen successfully!".format("{}_1".format(sheet_name)))
        pprint(response)

    except Exception as e:
        print("Error: {}".format(e))


def parse_col(cols):
    """
    Helper function to parse and retrieve column names
    :param cols:
    :return:
    """

    parsed_result = []
    parsed_col = re.split('[" ,]', str(cols))
    for e in parsed_col:
        if e is '':
            continue
        else:
            parsed_result.append(e)

    columns = []
    for i in range(0, len(parsed_result)):
        if i % 2:
            continue
        else:
            columns.append(parsed_result[i])

    return columns


def get_sheet_meta(creds, url):
    """
    Helper function to get goosle sheet metadata.
    :param creds:
    :param url:
    :return:
    """

    # get spreadsheet ID and name
    service = discovery.build('sheets', 'v4', credentials=creds)
    parts = parse.urlparse(url)

    res = dict()
    meta = []
    if parts.path.endswith('/edit'):
        path = parts.path[:-len('/edit')]
        meta.append(path.split('/')[-1])

    # spreadsheet ID
    spreadsheet_id = meta[0]

    # worksheet gid
    if parts.fragment.startswith('gid='):
        gid = parts.fragment[len('gid='):]
        meta.append(gid)

    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    properties = sheet_metadata.get('sheets')
    for i, item in enumerate(properties):
        title = item.get("properties").get('title')
        sheet_id = (item.get("properties").get('sheetId'))
        res[sheet_id] = title

    # get worksheet title
    for key, value in res.items():
        print(key, value)
        if str(key) == str(meta[1]):
            meta.append(value)

    return [meta[0], meta[2]]


def get_col_names(cursor, table):
    """
    Helper function to retrieve columns from  SQLite memory table
    :param cursor:
    :param table:
    :return:
    """
    cursor.execute('SELECT * FROM "{table}"'.format(table=table))
    return [member[0] for member in cursor.description]


def add_sheets(creds, gsheet_id, sheet_name):
    """
    Add new worksheet
    :param creds:
    :param gsheet_id:
    :param sheet_name:
    :return:
    """

    service = discovery.build('sheets', 'v4', credentials=creds)
    spreadsheets = service.spreadsheets()


    try:
        request_body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name,
                        'tabColor': {
                            'red': 0.44,
                            'green': 0.99,
                            'blue': 0.50
                        }
                    }
                }
            }]
        }

        response = spreadsheets.batchUpdate(
            spreadsheetId=gsheet_id,
            body=request_body
        ).execute()

        return response
    except Exception as e:
        print(e)
