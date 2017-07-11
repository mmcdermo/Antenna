# Copyright 2016 Morgan McDermott & Blake Allen

import unittest
import json
import os.path
from antenna.Storage import DynamoDBStorage
from antenna.Transformers import Item
from antenna.AWSManager import AWSManager

class TestStorage(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_dynamodb_storage(self):
        config = {
            "type": "DynamoDBStorage",
            "dynamodb_table_name": "test_table",
            "partition_key": "my_hash_key",
            "partition_key_format_string": "{category}-{url}"
        }
        item = {
            "category": "SomeCategory",
            "url": "http://google.com",
            "dict_test": {"corn": "husk"},
            "list_test": [1, 2, "corn"],
            "num": 4.02
        }

        manager = AWSManager()
        dynamostorage = DynamoDBStorage(manager, config)
        dynamo_item = dynamostorage.dynamo_item(Item(item_type="", payload=item))
        print(dynamo_item)
        self.assertEqual(dynamo_item['my_hash_key']['S'],
                         "%s-%s" % (item['category'], item['url']))
        for k in item:
            v = item[k]
            if isinstance(v, str):
                self.assertEqual(dynamo_item[k]['S'], v)
            elif isinstance(v, dict):
                d = json.loads(dynamo_item[k]['S'])
                for k in v:
                    self.assertEqual(d[k], v[k])
            elif isinstance(v, list):
                l = json.loads(dynamo_item[k]['S'])
                for i in range(len(v)):
                    self.assertEqual(l[i], v[i])
            else:
                self.assertEqual(dynamo_item[k]['N'], str(v))
