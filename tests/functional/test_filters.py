# Copyright 2016 Morgan McDermott & Blake Allen

import unittest
import json
import os.path
from antenna.Filters import UniqueDynamoDBFilter
from antenna.AWSManager import AWSManager

class TestFilters(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_unique_dynamodb_filter(self):
        config = {
            "dynamodb_table_name": "test_table",
            "primary_key": "my_hash_key",
            "primary_key_format_string": "{category}-{url}"
        }
        item = {
            "category": "SomeCategory",
            "url": "http://google.com"
        }

        manager = AWSManager()
        ufilter = UniqueDynamoDBFilter(manager, config)
        formatted = ufilter.format_key(item)
        self.assertEqual(formatted, "%s-%s" % (item['category'], item['url']))
