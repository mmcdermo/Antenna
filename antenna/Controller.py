# Copyright 2016 Morgan McDermott & Blake Allen"""
"""

The Controller class coordinates the creation of Sources and Transformers.

"""
import os
import os.path
from zipfile import ZipFile

import json
import time
import datetime
import importlib
from threading import Thread

import antenna.Sources as Sources
import antenna.Transformers as Transformers
import antenna.Filters as Filters
import antenna.Storage as Storage
import antenna.AWSManager as AWSManager
import antenna.ResourceManager as ResourceManager
import botocore

sourceClassMap = {
    'ArchivedRedditSubmissionsSource': Sources.ArchivedRedditSubmissionsSource,
    'RSSFeedSource': Sources.RSSFeedSource,
    'StaticFileSource': Sources.RSSFeedSource,
    'PaginatedAPISource': Sources.PaginatedAPISource,
}

transformerClassMap = {
    "IdentityTransformer": Transformers.IdentityTransformer,
    "DynamoDBStorage": Transformers.DynamoDBStorage
#    'ArticleScrapeTransformer': Sources.ArticleScrapeTransformer,
#    'ArticleAnalysisTransformer': Sources.ArticleAnalysisTransformer
}

storageClassMap = {
    "DynamoDBStorage": Storage.DynamoDBStorage
}

filterClassMap = {
    "UniqueDynamoDBFilter": Filters.UniqueDynamoDBFilter
}

class Controller(object):
    def __init__(self, config, source_path=None):
        self._defaults = {
            'local_controller': False,
            'local_jobs': False,
            'local_queue': False,
            'controller_schedule': 5, # Run the controller every N minutes
            'runtime': 10 # Maximum runtime defaults to 10s. This applies to transformer
                          # queue jobs only (typically the longest running portion)
        }

        self._source_path = source_path
        if self._source_path is None:
            self._source_path = os.path.dirname(os.path.abspath(__file__))

        self.validate_config(config)
        self.config = config
        self.local_queues = {}

        for key in config:
            setattr(self, key, config[key])

        for key in self._defaults:
            if key not in config:
                setattr(self, key, self._defaults[key])

        self._aws_manager = AWSManager.AWSManager()
        self._sqs = self._aws_manager._session.resource('sqs')
        self._sqs_queues = {}

        # Deploy cluster on initialization
        self._resource_manager = ResourceManager.ResourceManager(self)
        self._cluster = self._resource_manager.create_resource_cluster()

    def create_resources(self, force_update=False):
        try:
            if force_update:
                self._cluster.blocking_delete(verbose=True)
            self._cluster.blocking_deploy(verbose=True)
        except botocore.exceptions.ClientError as e:
            if "AlreadyExists" not in "%s" % e:
                raise e
            print("Stack already exists. Updating.")
            try:
                self._cluster.blocking_update(verbose = True)
            except botocore.exceptions.ClientError as e:
                if "No updates" not in "%s" % e:
                    raise e
                print("No update necessary.")

    def transformer_lambda_name(self, config):
        return "%sTransformer%s" % (self.config['project_name'],
                                           config['type'].replace(".", "_"))

    def source_lambda_name(self, config):
        return "%sSource%s" % (self.config['project_name'],
                                      config['type'].replace(".", "_"))

    def controller_lambda_name(self):
        return "%sController" % (self.config['project_name'])

    def create_lambda_functions(self):
        create_lambda_function(self.controller_lambda_name(),
                               self.get_lambda_role_arn(),
                               self._aws_manager.get_client('lambda'),
                               "lambda_handlers.controller_handler")


        for config in self.config['transformers']:
            create_lambda_function(self.transformer_lambda_name(config),
                                   self.get_lambda_role_arn(),
                                   self._aws_manager.get_client('lambda'),
                                   "lambda_handlers.transformer_handler")

        for config in self.config['sources']:
            create_lambda_function(self.source_lambda_name(config),
                                   self.get_lambda_role_arn(),
                                   self._aws_manager.get_client('lambda'),
                                   "lambda_handlers.transformer_handler")

    def schedule_controller_lambda(self):
        cloudwatch = self._aws_manager.get_client('events')
        lambdaclient = self._aws_manager.get_client('lambda')
        controller_function_arn = lambdaclient.get_function(
            FunctionName=self.controller_lambda_name())['Configuration']['FunctionArn']
        rule_name = '%sController' % self.config['project_name']
        cloudwatch.put_rule(
                Name=rule_name,
                ScheduleExpression='cron(0/%s * * * ? *)' % self.controller_schedule,
        )
        cloudwatch.put_targets(
            Rule=rule_name,
            Targets=[{
                "Id": rule_name,
                "Arn": controller_function_arn
            }]
        )

    def validate_config(self, config):
        required_keys = ['sources', 'transformers', 'project_name']
        for key in required_keys:
            if key not in config:
                raise Exception('Config must have key %s' % key)

    def get_sqs_queue(self, item_type):
        queue_name = self._resource_manager.queue_name(item_type)
        if item_type not in self._sqs_queues:
            url = self._aws_manager.get_client('sqs').get_queue_url(QueueName=queue_name)['QueueUrl']
            self._sqs_queues[item_type] = self._sqs.Queue(url)
        return self._sqs_queues[item_type]

    def drain_queues(self):
        queues = {}
        for item_type in self.item_types():
            queue = self.get_sqs_queue(item_type)
            for message in queue.receive_messages():
                message.delete()

    def dequeue_local_item(self, item_type):
        """
        Dequeue an item for local use and testing (replacement for SQS queue)
        """
        if item_type not in self.local_queues:
            return None
        if len(self.local_queues[item_type]) == 0:
            return None
        return self.local_queues[item_type].pop()

    def queue_local_item(self, item):
        """
        Queue an item for local use and testing (replacement for SQS queue)
        """
        if item.item_type not in self.local_queues:
            self.local_queues[item.item_type] = []
        self.local_queues[item.item_type].append(item)

    def instantiate_source(self, config):
        if config['type'] not in sourceClassMap:
            raise Exception('Unknown source type %s ' % config['type'])
        return sourceClassMap[config['type']](self._aws_manager, config)

    def run_source_job(self, config):
        items = []
        source = self.instantiate_source(config)
        for item in source.yield_items():
            if self.local_queue:
                self.queue_local_item(item)
            else:
                output_queue = self.get_sqs_queue(item.item_type)
                output_queue.send_message(
                    MessageBody=json.dumps(item.payload, indent=4))
                print("Created source item on queue %s" % item.item_type)

                if not self.filter_item(self.config.get("source_filters", []), item):
                    continue
                items.append(item)
                self.store_item(self.config.get("source_storage", []), item)
        return items

    def create_source_job(self, config):
        """
        Spawn a job for the given source config
        """
        source = self.instantiate_source(config)
        if source.has_new_data():
            if True == self.local_jobs:
                self.run_source_job(config)
            else:
                event = {
                    'controller_config': json.dumps(self.config),
                    'source_config': json.dumps(self.config['transformers'][0])
                }
                response = self._aws_manager.get_client('lambda').invoke(
                    FunctionName=self.source_lambda_name(config),
                    InvocationType='Event',
                    Payload=json.dumps(event)
                )

    def instantiate_transformer(self, config, source_path):
        if config['type'] not in transformerClassMap:
            transformer = self.import_transformer(config['type'], source_path)
            return transformer(AWSManager.AWSManager(), config)
            #except Exception as e:
            #    raise Exception('Unknown transformer type %s ' % config['type'])
        return transformerClassMap[config['type']](AWSManager.AWSManager(), config)

    def import_transformer(self, classpath, source_path):
        """
        Imports a Transformer
        e.g.) given the path "transformers.MyCustomTransformer", imports:
              import MyCustomTransformer from transformers
        """
        relpath = os.path.join(*(classpath.split('.')[:-1])) + '.py'
        fullpath = os.path.join(source_path, relpath)
        spec = importlib.util.spec_from_file_location(classpath.split(".")[-2], fullpath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, classpath.split(".")[-1])

    def instantiate_filter(self, filter_conf):
        if filter_conf["type"] not in filterClassMap:
            raise RuntimeError("Unknown filter type %s" % filter_conf["type"])
        return filterClassMap[filter_conf["type"]](AWSManager.AWSManager(), filter_conf)

    def filter_item(self, filter_configs, item):
        for filter_conf in filter_configs:
            filterObj = self.instantiate_filter(filter_conf)
            filterObj.filter(item)
        return True

    def instantiate_storage(self, storage_conf):
        if storage_conf["type"] not in storageClassMap:
            raise RuntimeError("Unknown storage type %s" % storage_conf["type"])
        return storageClassMap[storage_conf["type"]](AWSManager.AWSManager(), storage_conf)

    def store_item(self, storage_configs, item):
        """Store any produced items according to the a storage config found
           in a source/transformer config
        """
        for storage_conf in storage_configs:
            storageObj = self.instantiate_storage(storage_conf)
            storageObj.store_item(item)

    def run_transformer_job(self, config, input_item, source_path):
        transformer = self.instantiate_transformer(
            config, source_path)
        new_item = transformer.transform(input_item)
        input_queue = self.get_sqs_queue(input_item.item_type)
        client = self._aws_manager.get_client('sqs')
        resp = client.delete_message(
                QueueUrl=input_queue.url,
                ReceiptHandle=input_item.payload['sqs_receipt_handle']
            )
        if not self.filter_item(config.get("filters", []), new_item):
            return
        self.store_item(config.get("storage", []), new_item)

        output_queue = self.get_sqs_queue(new_item.item_type)
        output_queue.send_message(MessageBody=json.dumps(new_item.payload))
        print("Created new item on queue %s " % new_item.item_type)

    def item_from_message_payload(self, item_type, message, queue_url):
        """
        Bundles SQS message origin information into an item's paylaod.
        This permits remote worker to delete message that we retrieved locally.
        """
        payload = json.loads(message.body)

        payload['sqs_message_id'] = message.message_id
        payload['sqs_queue_url'] = queue_url
        payload['sqs_receipt_handle'] = message.receipt_handle
        return Sources.Item(item_type=item_type, payload=payload)

    def invoke_transformer_lambda(self, config, item):
        event = {
            'controller_config': json.dumps(self.config),
            'transformer_config': json.dumps(config),
            'item': json.dumps({"item_type": item.item_type, "payload": item.payload})
        }
        response = self._aws_manager.get_client('lambda').invoke(
            FunctionName=self.transformer_lambda_name(config),
            InvocationType='Event',
            Payload=json.dumps(event)
        )
        return response

    def create_transformer_job(self, config, item_type):
        """
        Spawn a job for the given transformer config
        """
        print("Running transformer stage for item type %s " % item_type)
        if True == self.local_queue:
            transformer = self.instantiate_transformer(config, self._source_path)
            for item_type in transformer.input_item_types():
                item = self.dequeue_local_item(item_type)
                while item != None:
                    new_item = transformer.transform(item)
                    self.queue_local_item(new_item)
                    item = self.dequeue_local_item(item_type)
        else:
            start = time.time()
            jobs = 0
            while time.time() - start < self.runtime:
                input_queue = self.get_sqs_queue(item_type)
                batch = []
                for message in input_queue.receive_messages():
                    # TODO: Ensure we aren't processing the same message twice
                    # for some long-running transformation
                    print("Acquired SQS message for item type %s" % (item_type))
                    item = self.item_from_message_payload(item_type, message, input_queue.url)
                    if self.local_jobs:
                        try:
                            jobs += 1
                            self.run_transformer_job(config, item)
                            print("Executing job %s" % jobs)
                        except Exception as e:
                            print("Error: failed to transform item with exception %s" %e)
                    else:
                        #Spin up lambda job for transformer + item
                        self.invoke_transformer_lambda(config, item)
                    print("Finished processing item with type %s" % item_type)
            # TODO:
            # Listen on appropriate SQS queue for tasks,
            # launching lambda jobs when either a time threshhold has been reached
            # or we have a full batch of items to be processed
            pass

    def item_types(self):
        types = []
        for transformer_config in self.config['transformers']:
            transformer = self.instantiate_transformer(transformer_config, self._source_path)
            types += transformer.input_item_types
            types += transformer.output_item_types
        return types

    def get_lambda_role_arn(self):
        role_name = self._cluster._mod_identifier(self._resource_manager.lambdaRole.get_id())
        client = self._aws_manager.get_client('iam')
        response = client.get_role(
                RoleName=role_name
        )
        return response['Role']['Arn']

        print(json.dumps(response, cls=MyEncoder))

    def run(self):
        for sourceConfig in self.sources:
            self.create_source_job(sourceConfig)

        # We create one transformer job for each transformer, with the same
        # maximum execution time as the Controller
        #
        # The transformers will exit as soon as there are no further messages on the queue
        #
        # This assumes that a single transformer lambda function
        # can keep up with the flow of incoming information.
        # If this isn't the case, we'll need to spawn multiple
        # transformer jobs for each transformer.
        transformers = []

        threads = []
        for transformerConfig in self.transformers:
            transformer = self.instantiate_transformer(transformerConfig, self._source_path)
            for item_type in transformer.input_item_types():
                t = Thread(
                    target=self.create_transformer_job,
                    args=[transformerConfig, item_type]
                )
                threads.append(t)
        [ t.start() for t in threads ]
        [ t.join() for t in threads ]

class MyEncoder(json.JSONEncoder):
    """
    JSON encoder that correctly encodes datetime.datetime objects
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(time.mktime(obj.timetuple()))
        return json.JSONEncoder.default(self, obj)

def create_lambda_function(name, role, client, handler, memory_size=128, source_dir=None):
    """
    Creates the lambda function if it doesn't exist.
    If it does exist, update it with the new zipfile.
    """
    zipfile = create_lambda_package(source_dir)
    contents = ""
    with open(zipfile, 'rb') as f:
        contents = f.read()

    try:
        res = client.create_function(
            FunctionName=name,
            Runtime='python3.6',
            Handler=handler,
            Code={
                'ZipFile': contents
            },
            Role=role,
            Timeout=10,
            MemorySize=memory_size
        )
    except Exception as e:
        if "already exist" in "%s" % e:
            #TODO: UPDATE
            return
        else:
            raise e

def recursively_add_files_to_zip(source_path, zipfile, base=""):
    for filename in os.listdir(source_path):
        if os.path.isdir(os.path.join(source_path, filename)):
            recursively_add_files_to_zip(os.path.join(source_path, filename), zipfile, os.path.join(base, filename))
        else:
            os.chmod(os.path.join(source_path, filename), '755')
            zipfile.write(os.path.join(source_path, filename), os.path.join(base, filename))

def cleanup_lambda_package(source_path=None):
    if source_path is None:
        source_path = os.path.dirname(os.path.abspath(__file__))
    os.remove(os.path.join(source_path, "lambda_package.zip"))

def create_lambda_package(source_path=None):
    if source_path is None:
        source_path = os.path.dirname(os.path.abspath(__file__))

    zipfilepath = os.path.join(source_path, "lambda_package.zip")
    if os.path.isfile(os.path.join(source_path, "lambda_package.zip")):
        return zipfilepath

    files = os.listdir(source_path)
    files = filter(lambda x: "py" in x and "pyc" not in x and "~" not in x, files)
    template_files = os.listdir(os.path.join(source_path, "lambda_template/"))

    with ZipFile(zipfilepath, 'w') as zipfile:
        for filename in files:
            os.chmod(os.path.join(source_path, filename), '755')
            zipfile.write(os.path.join(source_path, filename), filename)
        for filename in template_files:
            os.chmod(os.path.join(source_path, "lambda_template", filename), '755')
            zipfile.write(os.path.join(source_path, "lambda_template", filename), filename)
        recursively_add_files_to_zip(os.path.join(source_path, "lambda_env/"), zipfile)
        if source_path is not None:
            recursively_add_files_to_zip(source_path, zipfile)
    os.chmod(zipfilepath, '755')
    return zipfilepath
