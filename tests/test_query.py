# -*- coding: utf-8 -*-

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock

from collections import namedtuple
import unittest

import requests_mock
from six import BytesIO
from urllib3.response import HTTPResponse

from .context import (
    exceptions,
    execute,
    get_column_map,
    get_description_from_payload,
    LEADING,
    run_query,
    Type,
)


class QueryTestSuite(unittest.TestCase):

    @requests_mock.Mocker()
    def test_get_column_map(self, m):
        payload = {
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'B',
                        'label': 'cnt',
                        'type': 'number',
                        'pattern': 'General',
                    },
                ],
            },
        }
        m.get('http://docs.google.com/&tq=SELECT%20%2A%20LIMIT%200',
              json=payload)

        url = 'http://docs.google.com/'
        result = get_column_map(url)
        expected = {'country': 'A', 'cnt': 'B'}
        self.assertEqual(result, expected)

    @requests_mock.Mocker()
    def test_run_query(self, m):
        m.get('http://docs.google.com/&tq=SELECT%20%2A', json='ok')

        baseurl = 'http://docs.google.com/'
        query = 'SELECT *'
        result = run_query(baseurl, query)
        expected = 'ok'
        self.assertEqual(result, expected)

    @requests_mock.Mocker()
    def test_run_query_with_credentials(self, m):
        m.get('http://docs.google.com/&tq=SELECT%20%2A', json='ok')
        credentials = Mock()
        credentials.before_request = Mock()

        baseurl = 'http://docs.google.com/'
        query = 'SELECT *'
        result = run_query(baseurl, query, credentials)
        expected = 'ok'
        self.assertEqual(result, expected)

    @requests_mock.Mocker()
    def test_run_query_error(self, m):
        m.get(
            'http://docs.google.com/&tq=SELECT%20%2A',
            text='Error',
            status_code=500,
        )

        baseurl = 'http://docs.google.com/'
        query = 'SELECT *'
        with self.assertRaises(exceptions.ProgrammingError):
            run_query(baseurl, query)

    @requests_mock.Mocker()
    def test_run_query_leading(self, m):
        text = '{0}{1}'.format(LEADING, '"ok"')
        m.get('http://docs.google.com/&tq=SELECT%20%2A', text=text)

        baseurl = 'http://docs.google.com/'
        query = 'SELECT *'
        result = run_query(baseurl, query)
        expected = 'ok'
        self.assertEqual(result, expected)

    @requests_mock.Mocker()
    def test_run_query_no_encoding(self, m):
        raw = HTTPResponse(
            body=BytesIO('"ok"'.encode('utf-8')),
            preload_content=False,
            headers={
                'Content-type': 'application/json',
            },
            status=200,
        )
        m.get('http://docs.google.com/&tq=SELECT%20%2A', raw=raw)

        baseurl = 'http://docs.google.com/'
        query = 'SELECT *'
        result = run_query(baseurl, query)
        expected = 'ok'
        self.assertEqual(result, expected)

    def test_get_description_from_payload(self):
        payload = {
            'table': {
                'cols': [
                    {
                        'id': 'A',
                        'label': 'datetime',
                        'type': 'datetime',
                        'pattern': 'M/d/yyyy H:mm:ss',
                    },
                    {
                        'id': 'B',
                        'label': 'number',
                        'type': 'number',
                        'pattern': 'General',
                    },
                    {'id': 'C', 'label': 'boolean', 'type': 'boolean'},
                    {
                        'id': 'D',
                        'label': 'date',
                        'type': 'date',
                        'pattern': 'M/d/yyyy',
                    },
                    {
                        'id': 'E',
                        'label': 'timeofday',
                        'type': 'timeofday',
                        'pattern': 'h:mm:ss am/pm',
                    },
                    {'id': 'F', 'label': 'string', 'type': 'string'},
                ],
            },
        }
        result = get_description_from_payload(payload)
        expected = [
            ('datetime', Type.DATETIME, None, None, None, None, True),
            ('number', Type.NUMBER, None, None, None, None, True),
            ('boolean', Type.BOOLEAN, None, None, None, None, True),
            ('date', Type.DATE, None, None, None, None, True),
            ('timeofday', Type.TIMEOFDAY, None, None, None, None, True),
            ('string', Type.STRING, None, None, None, None, True),
        ]
        self.assertEqual(result, expected)

    @requests_mock.Mocker()
    def test_execute(self, m):
        header_payload = {
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'B',
                        'label': 'cnt',
                        'type': 'number',
                        'pattern': 'General',
                    },
                ],
            },
        }
        m.get(
            'http://docs.google.com/gviz/tq?headers=1&gid=0&'
            'tq=SELECT%20%2A%20LIMIT%200',
            json=header_payload,
        )
        query_payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'B',
                        'label': 'cnt',
                        'type': 'number',
                        'pattern': 'General',
                    },
                ],
                'rows': [{'c': [{'v': 'BR'}, {'v': 1.0, 'f': '1'}]}],
            },
        }
        m.get(
            'http://docs.google.com/gviz/tq?headers=1&gid=0&tq=SELECT%20%2A',
            json=query_payload,
        )

        query = 'SELECT * FROM "http://docs.google.com/"'
        headers = 1
        results, description = execute(query, headers)
        Row = namedtuple('Row', 'country cnt')
        self.assertEqual(results, [Row(country=u'BR', cnt=1.0)])
        self.assertEqual(
            description,
            [
                ('country', Type.STRING, None, None, None, None, True),
                ('cnt', Type.NUMBER, None, None, None, None, True),
            ],
        )

    @requests_mock.Mocker()
    def test_execute_with_processor(self, m):
        header_payload = {
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'B',
                        'label': 'cnt',
                        'type': 'number',
                        'pattern': 'General',
                    },
                ],
            },
        }
        m.get(
            'http://docs.google.com/gviz/tq?headers=1&gid=0&'
            'tq=SELECT%20%2A%20LIMIT%200',
            json=header_payload,
        )
        query_payload = {
            'status': 'ok',
            'table': {
                'cols': [
                    {
                        'id': 'count-A',
                        'label': 'count country',
                        'type': 'number',
                    },
                    {
                        'id': 'count-B',
                        'label': 'count cnt',
                        'type': 'number',
                    },
                ],
                'rows': [{'c': [{'v': 5.0}, {'v': 5.0}]}],
            },
        }
        m.get(
            'http://docs.google.com/gviz/tq?headers=1&gid=0&'
            'tq=SELECT%20COUNT(B)%2C%20COUNT(A)',
            json=query_payload,
        )

        query = 'SELECT COUNT(*) AS total FROM "http://docs.google.com/"'
        headers = 1
        results, description = execute(query, headers)
        Row = namedtuple('Row', 'total')
        self.assertEqual(results, [Row(total=5.0)])
        self.assertEqual(
            description,
            [('total', Type.NUMBER, None, None, None, None, True)],
        )

    def test_execute_bad_query(self):
        with self.assertRaises(exceptions.ProgrammingError):
            execute('SELECT ORDER BY FROM table')

    def test_execute_invalid_url(self):
        with self.assertRaises(exceptions.InterfaceError):
            execute('SELECT * FROM "http://example.com/"')

    @requests_mock.Mocker()
    def test_execute_gsheets_error(self, m):
        header_payload = {
            'table': {
                'cols': [
                    {'id': 'A', 'label': 'country', 'type': 'string'},
                    {
                        'id': 'B',
                        'label': 'cnt',
                        'type': 'number',
                        'pattern': 'General',
                    },
                ],
            },
        }
        m.get(
            'http://docs.google.com/gviz/tq?headers=1&gid=0&'
            'tq=SELECT%20%2A%20LIMIT%200',
            json=header_payload,
        )
        query_payload = {
            'status': 'error',
            'errors': [{'detailed_message': 'Error!'}],
        }
        m.get(
            'http://docs.google.com/gviz/tq?headers=1&gid=0&'
            'tq=SELECT%20COUNT(B)%2C%20COUNT(A)',
            json=query_payload,
        )

        query = 'SELECT COUNT(*) AS total FROM "http://docs.google.com/"'
        headers = 1
        with self.assertRaises(exceptions.ProgrammingError):
            execute(query, headers)
