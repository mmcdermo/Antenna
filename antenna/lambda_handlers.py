import os
import os.path
import json

from antenna.Controller import Controller
from antenna.Sources import Item

def transformer_handler(event, context):
    print("Transformer handler initialized")
    controller_config = json.loads(event['controller_config'])
    transformer_config = json.loads(event['transformer_config'])
    item_dict = json.loads(event['item'])
    item = Item(item_type=item_dict['item_type'], payload=item_dict['payload'])
    controller = Controller(controller_config, os.getcwd())

    #try:
    if True:
        print("Running transformer job")
        controller.run_transformer_job(
            transformer_config,
            item,
            os.getcwd())
        return {
            'status' : 'OK'
        }
    try:
        pass
    except Exception as e:
        msg = "Transformer failed to execute with error: %s" % e
        print(msg)
        return {
            'status' : 'error',
            'message': msg
        }

def aggregate_controller_handler(event, context):
    print("Transformer handler initialized")
    with open("./antenna.json", 'r') as f:
        controller_config = json.load(f)
    controller = Controller(controller_config, os.getcwd(), no_deploy=True)
    if True:
        print("Running transformer job")
        controller.run_aggregate_controller_job()
        return {
            'status' : 'OK'
        }
    try:
        pass
    except Exception as e:
        msg = "Transformer failed to execute with error: %s" % e
        print(msg)
        return {
            'status' : 'error',
            'message': msg
        }

def aggregate_transformer_handler(event, context):
    print("Transformer handler initialized")
    controller_config = json.loads(event['controller_config'])
    item_dict = json.loads(event['item'])
    item = Item(item_type=item_dict['item_type'],
                payload=item_dict['payload'])
    controller = Controller(controller_config, os.getcwd(), no_deploy=True)
    if True:
        print("Running transformer job")
        controller.run_aggregate_transformer_job(
            controller_config,
            item,
            os.getcwd()
        )
        return {
            'status' : 'OK'
        }
    try:
        pass
    except Exception as e:
        msg = "Transformer failed to execute with error: %s" % e
        print(msg)
        return {
            'status' : 'error',
            'message': msg
        }

def source_handler(event, context):
    controller_config = json.loads(event['controller_config'])
    source_config = json.loads(event['source_config'])
    controller = Controller(controller_config, os.getcwd())

    try:
        controller.run_source_job(source_config)
        return {
            'status' : 'OK'
        }
    except Exception as e:
        msg = "Source failed to execute with error: %s" % e
        print(msg)
        return {
            'status' : 'error',
            'message': msg
        }

def controller_handler(event, context):
    with open("./antenna.json", 'r') as f:
        controller_config = json.load(f)
    controller = Controller(controller_config, os.getcwd())
    controller.run()

def source_controller_handler(event, context):
    with open("./antenna.json", 'r') as f:
        controller_config = json.load(f)
    controller = Controller(controller_config, os.getcwd())
    controller.run_sources()

def transformer_controller_handler(event, context):
    with open("./antenna.json", 'r') as f:
        controller_config = json.load(f)
    controller = Controller(controller_config, os.getcwd())
    controller.run_transformers()
