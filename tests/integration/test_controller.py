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

class TestController(unittest.TestCase):
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
                    }],
                    "filters": [{
                        "type": "UniqueDynamoDBFilter",
                        "dynamodb_table_name": "antenna_test_table",
                        "partition_key": "my_hash_key",
                        "partition_key_format_string": "{url}"
                    }]
                }
            ]
        }

    def tearDown(self):
        controller = Controller(self.config)
        controller.drain_queues()

    def test_controller(self):
        controller = Controller(self.config)
        controller.run()

        resources = ResourceManager(controller)
        cluster = resources.createResourceCluster()
        #print(json.dumps(cluster.cloud_formation_template(), indent=4))
        #self.assertEqual(0, len(controller.local_queues["ArticleReference"]))
        #self.assertTrue(len(controller.local_queues["TransformedReference"]) > 3)

    def test_transformer_lambda_handler(self):
        controller = Controller(self.config)
        #controller.create_resources()
        controller.run_source_job(self.config['sources'][0])[0]
        input_queue = controller.get_sqs_queue(item.item_type)
        queue_item = None

        time.sleep(1)
        for message in input_queue.receive_messages():
            queue_item = controller.item_from_message_payload(item.item_type, message, input_queue.url)
            print("Message", message)
            print(queue_item.payload)
            event = {
                'controller_config': json.dumps(self.config),
                'transformer_config': json.dumps(self.config['transformers'][0]),
                'item': json.dumps({"item_type": queue_item.item_type, "payload": queue_item.payload})
            }
            transformer_handler(event, None)

    def test_source_lambda_handler(self):
        controller = Controller(self.config)
        controller.create_resources()
        event = {
            'controller_config': json.dumps(self.config),
            'source_config': json.dumps(self.config['sources'][0])
        }
        source_handler(event, None)

    def test_controller_lambda_handler(self):
        with open("./antenna.json", 'r') as f:
            controller = Controller(json.load(f))
        #controller.create_lambda_functions()
        controller_handler(None, None)

    def test_create_lambda_package(self):
        fn = create_lambda_package()
        zf = zipfile.ZipFile(fn)
        self.assertTrue(len(zf.infolist()) > 10)
        zf.close()

    def test_deploy(self):
        controller = Controller(self.config)

        # Create cluster if needed
        controller.create_resources()

        # Update Lambdas if needed
        controller.create_lambda_functions()

        # Schedule master lambda
        controller.schedule_controller_lambda()

    def test_lambda(self):
        controller = Controller(self.config)
        controller.create_resources()
        controller.create_lambda_functions()

        role_arn = controller.get_lambda_role_arn()
        client = controller._aws_manager.get_client('lambda')
        print("ARN: %s" % role_arn)

        function_name = "test%s" % random.randrange(100000000)

        res = create_lambda_function(function_name,
                                     role_arn,
                                     controller._aws_manager.get_client('lambda'),
                                     "lambda_handlers.transformer_handler")

        controller = Controller(self.config)
        item = controller.run_source_job(self.config['sources'][0])[0]
        input_queue = controller.get_sqs_queue(item.item_type)
        queue_item = None
        time.sleep(1)
        for message in input_queue.receive_messages():
            queue_item = controller.item_from_message_payload(item.item_type, message, input_queue.url)
            break

        event = {
            'controller_config': json.dumps(self.config),
            'transformer_config': json.dumps(self.config['transformers'][0]),
            'item': json.dumps({"item_type": queue_item.item_type, "payload": queue_item.payload})
        }

        response = client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(event)
        )
        payload = str(response['Payload'].read())
        print("Response payload", payload)
        self.assertEqual(json.loads(payload)['status'], 'OK')

        cleanup_lambda_package()
        response = client.delete_function(FunctionName=function_name)
