# Copyright 2016 Morgan McDermott & Blake Allen

import unittest
import json
import os.path
from antenna.Storage import DynamoDBStorage
from antenna.AWSManager import AWSManager

class TestStorage(unittest.TestCase):
    def setUp(self):
        self.config = {
            "dynamodb_table_name": "test_table",
            "partition_key": "my_hash_key",
            "partition_key_format_string": "{category}-{url}"
        }
        self.item = {
            "category": "SomeCategory",
            "url": "http://google.com",
            "num": 4.02
        }

    def tearDown(self):
        pass

    def test_dynamodb_storage(self):
        manager = AWSManager()
        dynamostorage = DynamoDBStorage(manager, self.config)
        dynamo_item = dynamostorage.dynamo_item(self.item)
        print(dynamo_item)
        self.assertEqual(dynamo_item['my_hash_key']['S'],
                         "%s-%s" % (item['category'], self.item['url']))
        for k in self.item:
            v = item[k]
            if isinstance(v, str):
                self.assertEqual(dynamo_item[k]['S'], v)
            else:
                self.assertEqual(dynamo_item[k]['N'], str(v))

    def test_external_resources(self):
        manager = AWSManager()
        dynamostorage = DynamoDBStorage(manager, self.config)
        resources = dynamostorage.external_resources()
        self.assertEqual(len(resources), 1)
