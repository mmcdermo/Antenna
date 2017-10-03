# Copyright 2016 Morgan McDermott

import unittest
import json
import time
import os.path
import random
import antenna.Sources as Sources
import zipfile
import os
from antenna.Controller import Controller, create_lambda_package, cleanup_lambda_package, create_lambda_function
from antenna.ResourceManager import ResourceManager
from antenna.lambda_handlers import transformer_handler, source_handler, controller_handler
from antenna.DataMapper import DataMapper

class TestBackfill(unittest.TestCase):
    def setUp(self):
        self.config = {
            'project_name': 'testAntenna42',
            'local_controller': True,
            'local_jobs': True,
            'local_queue': False,
            'sources': [
                {
                    "type": "RSSFeedSource",
                    "rss_feed_url": "https://qz.com/feed/"
                }
            ],
            "source_storage": [{
                "type": "DynamoDBStorage",
                "dynamodb_table_name": "antenna_test_table",
                "partition_key": "my_hash_key",
                "partition_key_format_string": "{url}"
            }],
            'transformers': [
                {
                    "type": "IdentityTransformer",
                    "input_item_types": ["ArticleReference"],
                    "output_item_types": ["TransformedReference"],
                    "storage": [{
                        "type": "DynamoDBStorage",
                        "dynamodb_table_name": "antenna_test_table",
                        "partition_key": "my_hash_key",
                        "partition_key_format_string": "{url}"
                    }]
                }
            ]
        }

    def test_backfill(self):
        source_path = os.path.join(*(["/"] + (__file__.split("/")[:-2]) + ["test_data"]))
        controller = Controller(self.config, source_path)
        #controller.create_resources()
        #item = controller.run_source_job(self.config['sources'][0])[0]
        mapper = DataMapper(controller)

        stats = mapper.local_backfill("antenna_test_table",
                                      "ArticleReference",
                                      "IdentityTransformer",
                                      required_null_field="source_url",
                                      limit=10
        )
        self.assertEqual(stats['number_items_backfilled'], 0)

        stats = mapper.local_backfill("antenna_test_table",
                                      "ArticleReference",
                                      "IdentityTransformer",
                                      required_null_field=None,
                                      limit=10
        )
        self.assertTrue(stats['number_items_backfilled'] > 0)
