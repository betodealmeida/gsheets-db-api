# -*- coding: utf-8 -*-

import unittest

from moz_sql_parser import parse
import pyparsing

from .context import format_gsheet_error, format_moz_error


class UtilsTestSuite(unittest.TestCase):

    def test_format_moz_error(self):
        query = 'SELECT ))) FROM table'
        with self.assertRaises(pyparsing.ParseException) as context:
            parse(query)

        result = format_moz_error(query, context.exception)
        expected = (
            'SELECT ))) FROM table\n'
            '       ^\n'
            'Expected {{expression1 [{[as] column_name1}]} | "*"} '
            '(at char 7), (line:1, col:8)'
        )
        self.assertEqual(result, expected)

    def test_format_gsheet_error(self):
        query = 'SELECT A + B FROM "http://docs.google.com"'
        translated_query = 'SELECT A + B'
        errors = [{
            'reason': 'invalid_query',
            'detailed_message': (
                "Invalid query: Can't perform the function sum on values that "
                "are not numbers"
            ),
            'message': 'INVALID_QUERY',
        }]

        result = format_gsheet_error(query, translated_query, errors)
        expected = (
            'Original query:\n'
            'SELECT A + B FROM "http://docs.google.com"\n\n'
            'Translated query:\n'
            'SELECT A + B\n\n'
            'Error:\n'
            "Invalid query: Can't perform the function sum on values that "
            "are not numbers"
        )
        self.assertEqual(result, expected)

    def test_format_gsheet_error_caret(self):
        query = 'SELECT A IS NULL FROM "http://docs.google.com"'
        translated_query = 'SELECT A IS NULL'
        errors = [{
            'reason': 'invalid_query',
            'detailed_message': (
                'Invalid query: PARSE_ERROR: Encountered " "is" "IS "" at '
                'line 1, column 10.\nWas expecting one of:\n'
                '    <EOF> \n'
                '    "where" ...\n'
                '    "group" ...\n'
                '    "pivot" ...\n'
                '    "order" ...\n'
                '    "skipping" ...\n'
                '    "limit" ...\n'
                '    "offset" ...\n'
                '    "label" ...\n'
                '    "format" ...\n'
                '    "options" ...\n'
                '    "," ...\n'
                '    "*" ...\n'
                '    "+" ...\n'
                '    "-" ...\n'
                '    "/" ...\n'
                '    "%" ...\n'
                '    "*" ...\n'
                '    "/" ...\n'
                '    "%" ...\n'
                '    "+" ...\n'
                '    "-" ...\n'
                '    '
            ),
            'message': 'INVALID_QUERY',
        }]

        result = format_gsheet_error(query, translated_query, errors)
        expected = (
            'Original query:\n'
            'SELECT A IS NULL FROM "http://docs.google.com"\n\n'
            'Translated query:\n'
            'SELECT A IS NULL\n\n'
            'Error:\n'
            'SELECT A IS NULL\n'
            '         ^\n'
            'Invalid query: PARSE_ERROR: Encountered " "is" "IS "" at line 1, '
            'column 10.\n'
            'Was expecting one of:\n'
            '    <EOF> \n'
            '    "where" ...\n'
            '    "group" ...\n'
            '    "pivot" ...\n'
            '    "order" ...\n'
            '    "skipping" ...\n'
            '    "limit" ...\n'
            '    "offset" ...\n'
            '    "label" ...\n'
            '    "format" ...\n'
            '    "options" ...\n'
            '    "," ...\n'
            '    "*" ...\n'
            '    "+" ...\n'
            '    "-" ...\n'
            '    "/" ...\n'
            '    "%" ...\n'
            '    "*" ...\n'
            '    "/" ...\n'
            '    "%" ...\n'
            '    "+" ...\n'
            '    "-" ...'
        )
        self.assertEqual(result, expected)
