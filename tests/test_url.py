# -*- coding: utf-8 -*-

import unittest

from .context import extract_url, get_url


class UrlTestSuite(unittest.TestCase):

    def test_extract_url(self):
        query = 'SELECT * FROM "http://docs.google.com"'
        result = extract_url(query)
        expected = 'http://docs.google.com'
        self.assertEqual(result, expected)

    def test_get_url(self):
        url = 'http://docs.google.com'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://docs.google.com/gviz/tq?headers=1&gid=10'
        self.assertEqual(result, expected)

    def test_remove_edit(self):
        url = 'http://docs.google.com/edit#gid=0'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://docs.google.com/gviz/tq?headers=1&gid=0'
        self.assertEqual(result, expected)

    def test_url_gid_qs(self):
        url = 'http://docs.google.com/?gid=0'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://docs.google.com/gviz/tq?headers=1&gid=0'
        self.assertEqual(result, expected)

    def test_url_headers_qs(self):
        url = 'http://docs.google.com/?gid=0&headers=2'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://docs.google.com/gviz/tq?headers=2&gid=0'
        self.assertEqual(result, expected)

    def test_sheet_name(self):
        url = 'http://docs.google.com/?gid=0&headers=2&sheet=table'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://docs.google.com/gviz/tq?headers=2&sheet=table'
        self.assertEqual(result, expected)

    def test_extract_url_bad_sql(self):
        query = 'SELECTSELECTSELECT'
        result = extract_url(query)
        self.assertIsNone(result)

    def test_extract_url_using_regex(self):
        query = 'INVALID FROM "http://docs.google.com"'
        result = extract_url(query)
        expected = 'http://docs.google.com'
        self.assertEqual(result, expected)
