# -*- coding: utf-8 -*-

import unittest

from .context import extract_url, get_url


class UrlTestSuite(unittest.TestCase):

    def test_extract_url(self):
        query = 'SELECT * FROM "http://example.com"'
        result = extract_url(query)
        expected = 'http://example.com'
        self.assertEqual(result, expected)

    def test_get_url(self):
        url = 'http://example.com'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://example.com/gviz/tq?headers=1&gid=10'
        self.assertEqual(result, expected)

    def test_remove_edit(self):
        url = 'http://example.com/edit#gid=0'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://example.com/gviz/tq?headers=1&gid=0'
        self.assertEqual(result, expected)

    def test_url_gid_qs(self):
        url = 'http://example.com/?gid=0'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://example.com/gviz/tq?headers=1&gid=0'
        self.assertEqual(result, expected)

    def test_url_headers_qs(self):
        url = 'http://example.com/?gid=0&headers=2'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://example.com/gviz/tq?headers=2&gid=0'
        self.assertEqual(result, expected)

    def test_sheet_name(self):
        url = 'http://example.com/?gid=0&headers=2&sheet=table'
        result = get_url(url, headers=1, gid=10, sheet=None)
        expected = 'http://example.com/gviz/tq?headers=2&sheet=table'
        self.assertEqual(result, expected)
