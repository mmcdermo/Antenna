# Copyright 2016 Morgan McDermott & Blake Allen

import unittest
import json
import os.path
from Antenna.Sources import StaticFileSource, RSSFeedSource
from Antenna.AWSManager import AWSManager

class TestSources(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_local_file_source(self):
        config = {
            'source_url': 'https://www.gutenberg.org/files/54386/54386-0.txt',
            's3_bucket_name': 'antennatest42',
            'destination_key': 'gutenberg.txt',
        }
        manager = AWSManager()

        # Ensure object does not exist before we move forward
        client = manager.get_client('s3')
        client.delete_object(Bucket=config['s3_bucket_name'],
                             Key=config['destination_key'])

        source = StaticFileSource(manager, config)
        self.assertTrue(source.has_new_data())

        items = list(source.yield_items())
        self.assertEqual(1, len(items))
        self.assertFalse(source.has_new_data())

    def test_rss_feed_source(self):
        config = {
            "rss_feed_url": "https://qz.com/feed/"
        }
        manager = AWSManager()
        source = RSSFeedSource(manager, config)
        self.assertTrue(source.has_new_data())

        items = list(source.yield_items())
        self.assertTrue(len(items) > 3)
        for item in items:
            self.assertTrue(len(item.payload['url']) > 10)
            self.assertTrue(len(item.payload['title']) > 10)

        # TODO - need to implement state saving mechanism for this to be accurate
        # self.assertFalse(source.has_new_data())
