# Copyright 2016 Morgan McDermott & Blake Allen
"""
Storage classes persist the output of sources or transformers.

Transformers/Sources produce items, they're filtered, and then finally persisted.

  Transformer/Source ----> Filters -----> Storage

"""
import json

import redleader.resources as r
from antenna.ResourceManager import ResourceManager

class Storage(object):
    def __init__(self, aws_manager, params):
        self._required_class_keywords = ["type"]
        self._excluded_item_properties = [
            "sqs_message_id", "sqs_queue_url", "sqs_receipt_handle"
        ]
        self._optional_keywords = getattr(self, "_optional_keywords", [])

        # Validate given parameters
        self._aws_manager = aws_manager
        self._validate_params(params)

        # Attach parameters to this object
        self.params = params
        for param in self._defaults:
            setattr(self, param, self._defaults[param])
        for param in params:
            setattr(self, param, params[param])

    def _validate_params(self, params):
        for param in self._required_keywords + self._required_class_keywords:
            if param not in params:
                raise Exception("Missing parameter %s for storage stage %s" %
                                (param, self.__class__.__name__))
        for param in params:
            if param not in self._required_keywords and \
               param not in self._required_class_keywords and \
               param not in self._optional_keywords:
                raise Exception("Unknown parameter %s for storage %s" %
                                (param, self.__class__.__name__))

    def store_item(self, item):
        raise NotImplementedError


class DynamoDBStorage(Storage):
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "dynamodb_table_name",
        ]
        self._optional_keywords = [
            "exclude_properties",
            "property_mapping",
            "partition_key",
            "partition_key_format_string",
            "range_key",
            "range_key_format_string",
            "range_key_type",
            "update_if_exists"
        ]
        self._defaults = {
            "update_if_exists": True,
            "range_key_type": "N",
        }
        super(DynamoDBStorage, self).__init__(aws_manager, params)

    def external_resources(self):
        table_config = ResourceManager.dynamo_key_schema(
            self.partition_key,
            range_key_name=getattr(self, "range_key", None),
            range_key_type=getattr(self, "range_key_type", None)
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

    @staticmethod
    def from_dynamo_dict(dynamo_dict):
        """
        {"foo": {"S": "bar"}} => {"foo": "bar"}
        """
        d = {}
        for k in dynamo_dict:
            ty_key = list(dynamo_dict[k].keys())[0]
            value = dynamo_dict[k][ty_key]
            if ty_key == "N":
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
            d[k] = value
        return d

    @staticmethod
    def dynamo_dict(orig):
        """
        Convert an ordinary dictionary into a dynamo-compatible attribute
        definition dictionary.

        {"foo": "bar"} => {"foo": {"S": "bar"}}

        Current supports:
        "N": Numeric values (float, int)
        "S": Strings
        "BOOL": Boolean values
        """
        ditem = {}
        for key in orig:
            dynamo_value = {}
            value = orig[key]
            dynamo_type = "S"
            if isinstance(value, bool):
                dynamo_type = "S"
                dynamo_value[dynamo_type] = "\"" + str(json.dumps(value)) + "\""
            elif isinstance(value, float) or isinstance(value, int):
                dynamo_type = "N"
                dynamo_value[dynamo_type] = json.dumps(value)
            elif isinstance(value, str):
                dynamo_value[dynamo_type] = value
            else:
                dynamo_value[dynamo_type] = json.dumps(value)
            if isinstance(value, bool) or len(dynamo_value[dynamo_type]):
                ditem[key] = dynamo_value
        return ditem

    def dynamo_item(self, item):
        """
        Transform a consumed item into a dynamodb entry
        """
        filtered = {k: v for k,v in item.payload.items() if k not in self._excluded_item_properties}
        ditem = DynamoDBStorage.dynamo_dict(filtered)

        # Set the primary key if applicable
        if hasattr(self, "partition_key"):
            ditem[self.partition_key] = {'S': self.format_key(item)}

        return ditem

    def store_item(self, item):
        if self.update_if_exists == False:
            return self.insert_fresh_item(item)
        else:
            return self.insert_or_update_item(item)

    def insert_fresh_item(self, item):
        raise NotImplementedError("No support for update_if_exists=False yet")

    def insert_or_update_item(self, item):
        ddb = self._aws_manager.get_client('dynamodb')

        # Attempt to retrieve old item.
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
        base_item = {}
        if 'Items' in res and len(res['Items']) > 0:
            base_item = res['Items'][0]

        # Merge the old and new items together
        new_item = self.dynamo_item(item)
        for k in new_item:
            base_item[k] = new_item[k]

        # Update the item in dynamo
        return ddb.put_item(
            TableName=self.dynamodb_table_name,
            Item=new_item
        )
