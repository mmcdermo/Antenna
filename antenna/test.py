import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

sess = boto3.session.Session(profile_name="signal", region_name="us-west-1")

ddb = sess.client('dynamodb')

res = ddb.describe_table(
    TableName="harvest_items"
    )
print(res)

res = ddb.query(
    TableName="harvest_items",
    KeyConditionExpression='#URL = :item_url',
    ExpressionAttributeNames = {
        "#URL": "url"
    },
    ExpressionAttributeValues={
        ':item_url': {
            'S': 'http://corn.com'
        }
    }
)
print(json.dumps(res, indent=4))
