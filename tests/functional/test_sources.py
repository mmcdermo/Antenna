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

    def test_invalid_config(self):
        config = {}
        manager = AWSManager()
        try:
            source = StaticFileSource(manager, config)
            self.assertEqual(False, "Source should have thrown exception given empty config")
        except Exception as e:
            pass

    def test_defaults(self):
        config = {
            'source_url': 'https://www.gutenberg.org/files/54386/54386-0.txt',
            's3_bucket_name': 'antennatest42',
            'destination_key': 'gutenberg.txt',
        }
        manager = AWSManager()
        source = StaticFileSource(manager, config)
        self.assertEqual(source._defaults['item_type'], source.item_type)
