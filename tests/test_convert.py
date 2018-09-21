# -*- coding: utf-8 -*-

from collections import namedtuple
import datetime
import unittest

from .context import convert_rows


class ConvertTestSuite(unittest.TestCase):

    payload = {
        "version": "0.6",
        "reqId": "0",
        "status": "ok",
        "sig": "1788543417",
        "table": {
            "cols": [
                {
                    "id": "A",
                    "label": "datetime",
                    "type": "datetime",
                    "pattern": "M/d/yyyy H:mm:ss",
                },
                {
                    "id": "B",
                    "label": "number",
                    "type": "number",
                    "pattern": "General",
                },
                {
                    "id": "C",
                    "label": "boolean",
                    "type": "boolean",
                },
                {
                    "id": "D",
                    "label": "date",
                    "type": "date",
                    "pattern": "M/d/yyyy",
                },
                {
                    "id": "E",
                    "label": "timeofday",
                    "type": "timeofday",
                    "pattern": "h:mm:ss am/pm",
                },
                {
                    "id": "F",
                    "label": "string",
                    "type": "string",
                },
            ],
            "rows": [
                {
                    "c": [
                        {"v": "Date(2018,8,1,0,0,0)", "f": "9/1/2018 0:00:00"},
                        {"v": 1.0, "f": "1"},
                        {"v": True, "f": "TRUE"},
                        {"v": "Date(2018,0,1)", "f": "1/1/2018"},
                        {"v": [17, 0, 0, 0], "f": "5:00:00 PM"},
                        {"v": "test"},
                    ],
                },
                {
                    "c": [
                        None,
                        {"v": 1.0, "f": "1"},
                        {"v": True, "f": "TRUE"},
                        None,
                        None,
                        {"v": "test"},
                    ],
                },
            ],
        },
    }

    def test_convert(self):
        cols = self.payload['table']['cols']
        rows = self.payload['table']['rows']
        result = convert_rows(cols, rows)
        Row = namedtuple(
            'Row', 'datetime number boolean date timeofday string')
        expected = [
            Row(
                datetime=datetime.datetime(2018, 9, 1, 0, 0),
                number=1.0,
                boolean=True,
                date=datetime.date(2018, 1, 1),
                timeofday=datetime.time(17, 0),
                string='test',
            ),
            Row(
                datetime=None,
                number=1.0,
                boolean=True,
                date=None,
                timeofday=None,
                string='test',
            ),
        ]
        self.assertEqual(result, expected)
