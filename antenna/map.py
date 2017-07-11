import boto3
import datetime
import json
import time

table_name='collector_articles'

def from_dynamo_dict(dynamo_dict):
    """
    {"foo": {"S": "bar"}} => {"foo": "bar"}
    """
    d = {}
    for k in dynamo_dict:
        ty = list(dynamo_dict[k].keys())[0]
        if ty == 'S':
            d[k] = dynamo_dict[k][ty]
        elif ty == 'N':
            d[k] = float(dynamo_dict[k][ty])
    return d

def dynamo_dict(orig):
    """
    Convert an ordinary dictionary into a dynamo-compatible attribute
    definition dictionary.

    {"foo": "bar"} => {"foo": {"S": "bar"}}
    """
    ditem = {}
    for key in orig:
        dynamo_value = {}
        value = orig[key]
        dynamo_type = "S"
        if isinstance(value, float) or isinstance(value, int):
            dynamo_type = "N"
            dynamo_value[dynamo_type] = json.dumps(value)
        elif isinstance(value, str):
            dynamo_value[dynamo_type] = value
        else:
            dynamo_value[dynamo_type] = json.dumps(value)
        if len(dynamo_value[dynamo_type]):
            ditem[key] = dynamo_value
    return ditem

def publish_map_fun(x):
    if 'week_published' in x:
        return None
    week = datetime.date.fromtimestamp(int(x['time_published'])).isocalendar()
    x['week_published'] = "%s_%s" % (week[0], week[1])
    print(x['week_published'])
    return x

def mapscan(table_name, map_fun, limit):
    client = boto3.client('dynamodb')
    res = client.scan(TableName=table_name, Limit=limit)
    total = 0
    while total < limit or 'Items' not in res or len(res['Items']) == 0:
        for item in res['Items']:
            total += 1
            new_item = map_fun(from_dynamo_dict(item))
            if new_item != None:
                client.put_item(
                    TableName=table_name,
                    Item=dynamo_dict(new_item)
                )
        print("Last Evaluated Key: ", res['LastEvaluatedKey'])
        time.sleep(10)
        res = client.scan(TableName=table_name, Limit=limit, ExclusiveStartKey=res['LastEvaluatedKey'])


def main():
    mapscan(table_name, map_fun, 2000)

if __name__ == '__main__':
    main()

"""

  API server:
    http://elastic-beanstalkjk897472.us-west2.elasticbeanstalk.amazon.com/api/models/DiscoverModel/discover_search/
  Per-model URL format:
  http://fdsjkfds.us-west2.elasticbeanstalk.amazon.com/api/DiscoverModel/discover_search/



"""
