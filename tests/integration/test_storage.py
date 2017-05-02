# Copyright 2016 Morgan McDermott & Blake Allen

import unittest
import json
import os.path
from antenna.Storage import DynamoDBStorage
from antenna.AWSManager import AWSManager

class TestStorage(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_dynamodb_storage(self):
        config = {
            "dynamodb_table_name": "test_table",
            "primary_key": "my_hash_key",
            "primary_key_format_string": "{category}-{url}"
        }
        item = {
            "category": "SomeCategory",
            "url": "http://google.com",
            "num": 4.02
        }

        manager = AWSManager()
        dynamostorage = DynamoDBStorage(manager, config)
        dynamo_item = dynamostorage.dynamo_item(item)
        print(dynamo_item)
        self.assertEqual(dynamo_item['my_hash_key']['S'],
                         "%s-%s" % (item['category'], item['url']))
        for k in item:
            v = item[k]
            if isinstance(v, str):
                self.assertEqual(dynamo_item[k]['S'], v)
            else:
                self.assertEqual(dynamo_item[k]['N'], str(v))
