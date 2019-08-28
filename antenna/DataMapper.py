import os
import json
from antenna.Transformers import Item
from antenna.Storage import DynamoDBStorage

class DataMapper():
    def __init__(self, controller):
        self.controller = controller
        self.resource_manager = controller._resource_manager

    def local_backfill(self,
                       dynamodb_table_name,
                       output_item_type,
                       transformer_type,
                       index_name=None,
                       partition_key=None,
                       partition_key_value=None,
                       required_null_field=None,
                       limit=False,
                       verbose=True,
                       min_key=None,
                       min_value=None
    ):
        client = self.controller._aws_manager.get_client('dynamodb')

        resp = None

        if(required_null_field == None):
            resp = client.scan(TableName=dynamodb_table_name, ScanIndexForward=False)
        else:
            attr_values = {
                ":pkeyvalue": {"S": partition_key_value},
                ":minvalue" : {"N" : min_value}
            }
            print("SCAN WITH", "attribute_not_exists(" + required_null_field + ")")
            print(partition_key + " = " + partition_key_value + " and " + min_key + " > " + min_value )
            resp = client.query(
                TableName=dynamodb_table_name,
                IndexName=index_name,
                ExpressionAttributeValues=attr_values ,
                FilterExpression="attribute_not_exists(" + required_null_field + ")", # and attribute_exists(" + min_key + ") and " + min_key + " > :minvalue",
                #attribute_exists(" + min_key + ")
                KeyConditionExpression= partition_key + " = :pkeyvalue and " + min_key + " > :minvalue",
                ScanIndexForward=False
                #FilterExpression="attribute_not_exists(" +
                #required_null_field + ") AND "+ partition_key +
                #" = :pkeyvalue"
            )

        print("RESP RETRIEVED")
        last_evaluated_key = resp.get('LastEvaluatedKey', None)

        # Note that this selection mechanism will be incorrect
        # if multiple transformers of the same type are present
        transformer_config = {}
        for conf in self.controller.config['transformers']:
            if conf['type'] == transformer_type:
                transformer_config = conf

        i = 0
        while last_evaluated_key is not None:
            print("Continuing scan...")

            for item in resp['Items']:
                d = DynamoDBStorage.from_dynamo_dict(item)
                transformed = self.controller.run_transformer_job(transformer_config,
                                                    Item(payload=d),
                                                    os.getcwd())
                if transformed is not None:
                    print(json.dumps(transformed.payload, indent=4)[0:100])
                    i += 1
                else:
                    print("Filtered item", transformed)
                print("Transformed " + str(i) + " items")

            if(required_null_field == None):
                resp = client.scan(
                    ExclusiveStartKey=last_evaluated_key,
                    TableName=dynamodb_table_name,
                    ScanIndexForward=False
                )
            else:
                resp = client.query(
                    TableName=dynamodb_table_name,
                    IndexName=index_name,
                    ExpressionAttributeValues= {
                        ":pkeyvalue": {"S": partition_key_value}
                    },
                    FilterExpression="attribute_not_exists(" + required_null_field + ")",
                    KeyConditionExpression=partition_key + " = :pkeyvalue",
                    ExclusiveStartKey=last_evaluated_key,
                    ScanIndexForward=False
                )

            if 'LastEvaluatedKey' not in resp:
                print("Response has no LastEvaluatedKey. Terminating backfill")
                return

            last_evaluated_key = resp['LastEvaluatedKey']
