# Copyright 2016 Morgan McDermott & Blake Allen

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

class TestTransformers(unittest.TestCase):
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
            'transformers': [
                {
                    "type": "transformers.CustomTestTransformer"
                }
            ]
        }
        pass

    def tearDown(self):
        pass

    def test_load_custom_transformer(self):
        source_path = os.path.join(*(["/"] + (__file__.split("/")[:-2]) + ["test_data"]))
        controller = Controller(self.config, source_path)
        module = controller.import_transformer("transformers.CustomTestTransformer",
                                               source_path)

    def test_custom_transformer(self):
        source_path = os.path.join(*(["/"] + (__file__.split("/")[:-2]) + ["test_data"]))
        controller = Controller(self.config, source_path)
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

        controller = Controller(self.config, source_path)
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
