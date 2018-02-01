# Copyright 2016 Morgan McDermott & Blake Allen
"""
Filters perform post-processing for any source or transformer.

Filters simply remove items from the pipeline, and are executed
immediately after item production.
"""
from antenna.Transformers import Transformer
import redleader.resources as r
from antenna.ResourceManager import ResourceManager
from boto3.dynamodb.conditions import Key, Attr


class Filter(Transformer):
    def __init__(self, aws_manager, params):
        # Validate given parameters
        self._meta_keywords = ["type"]
        self._validate_params(params)

        # Attach parameters to this object
        self.params = params
        for d in self._defaults:
            setattr(self, d, self._defaults[d])
        for param in params:
            setattr(self, param, params[param])

        self._aws_manager = aws_manager

    def _validate_params(self, params):
        for param in self._required_keywords:
            if param not in params:
                raise Exception("Missing parameter %s for filter %s" %
                                (param, self.__class__.__name__))
        for param in params:
            if param not in self._meta_keywords and \
               param not in self._required_keywords and \
               (not hasattr(self, "_optional_keywords") or \
                param not in self._optional_keywords):
                raise Exception("Unknown parameter `%s` for filter %s" %
                                (param, self.__class__.__name__))

    def filter(self):
        raise NotImplementedError

    def external_resources(self):
        return []

class UniqueDynamoDBFilter(Filter):
    """Filters out any items that are already referenced in DynamoDB

    `hash_key` specifies the primary DynamoDB key
    `partition_key_format_string` specifies how to construct the primary DynamoDB key
                                from a given ArticleReference item

    Currently only supports string keys.
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "dynamodb_table_name",
            "partition_key",
            "partition_key_format_string",
        ]
        self._optional_keywords = [
            "range_key",
            "range_key_type",
            "range_key_format_string"
        ]
        self._defaults = {
            "range_key_type": "N"
        }
        super(UniqueDynamoDBFilter, self).__init__(aws_manager, params)

    def external_resources(self):
        table_config = ResourceManager.dynamo_key_schema(
            self.partition_key,
            range_key_name=self.range_key if hasattr(self, "range_key") else None,
            range_key_type=self.range_key_type
        )
        table_resource = r.DynamoDBTableResource(
            self._aws_manager, self.dynamodb_table_name,
            attribute_definitions=table_config['attribute_definitions'],
            key_schema=table_config['key_schema'],
            write_units=5, read_units=5
        )
        return [table_resource]

    def format_key(self, item):
        """
        Produce the primary key by replacing item properties with their values.

        I.e) given item = {"name": "car", "desc": "..."},
                   partition_key_format_string = "{name}-primary-key"
                   => format_key(item, partition_key_format_string) = "car-primary-key"
        """
        base = self.partition_key_format_string
        for k in item.payload:
            base = base.replace("{%s}" % k, str(item.payload[k]))
        return base

    def ddb_row_exists(self, item):
        ddb = self._aws_manager.get_client('dynamodb')
        res = ddb.query(
            TableName=self.dynamodb_table_name,
            KeyConditionExpression='#PLACEHOLDER = :val',
            # We'll use a placeholder name in case our key is a dynamo
            # reserved keyword (very common)
            ExpressionAttributeNames = {
                "#PLACEHOLDER": self.partition_key
            },
            ExpressionAttributeValues={
                ':val': {
                    'S': self.format_key(item)
                }
            }
        )
        return 'Items' in res and len(res['Items']) > 0

    def filter(self, item):
        return not self.ddb_row_exists(item)
