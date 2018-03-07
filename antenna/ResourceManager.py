import json
from collections import OrderedDict
from functools import reduce
import antenna.AWSManager
from redleader.cluster import Cluster, AWSContext
import redleader.resources as r

class ResourceManager(object):
    """
    ResourceManager coordinates AWS resources for
    an Antenna controller
    """
    def __init__(self, controller):
        self._controller = controller

    def _cluster_name(self):
        return self._controller.project_name.replace("_", "").replace("-", "") + "Cluster"

    def queue_name(self, item_type):
        return self._controller.project_name + item_type + "Queue"

    def dead_letter_queue_name(self, item_type):
        return self._controller.project_name + item_type + "DeadLetterQueue"

    def nested_resources(self, conf):
        """
        Extracts resources from nested `filters` and `storage` objects
        """
        resources = []
        # Add filter resources
        filter_confs = conf.get("filters", [])
        filter_resources = map(lambda x: self._controller.\
                               instantiate_filter(x).external_resources(), filter_confs)
        resources += reduce(lambda x, y: x + y, filter_resources, []) # flatten list

        # Add storage resources
        storage_confs = conf.get("storage", [])
        if not isinstance(storage_confs, list):
            raise RuntimeError("`storage` configuration parameter must be a list of storage backends.")
        storage_resources = map(lambda x: self._controller.\
                                instantiate_storage(x).external_resources(), storage_confs)
        resources += reduce(lambda x, y: x + y, storage_resources, []) # flatten list
        return resources

    def aws_resources_needed(self):
        """
        Returns a list of external AWS resources needed for all sources and transformers
        Each resource is an object from  RedLeader.resources
        """
        resources = []
        for source_config in self._controller.config['sources']:
            resources += self._controller.instantiate_source(
                source_config, skip_loading_state=True).external_resources()
            resources += self.nested_resources(source_config)
        for transformer_config in self._controller.config['transformers']:
            resources += self._controller.instantiate_transformer(
                transformer_config,
                self._controller._source_path
            ).external_resources()
            print("Generated transformer resources", resources)
            resources += self.nested_resources(transformer_config)
        constructed_source_conf = {
            "storage": self._controller.config['source_storage'],
            "filters": getattr(self._controller.config, 'source_filters', [])
        }
        resources += self.nested_resources(constructed_source_conf)
        return resources

    def dynamo_table_name(self, postfix):
        return self._controller.project_name + postfix

    @staticmethod
    def dynamo_key_schema(partition_key_name,
                          range_key_name=None,
                          partition_key_type='S',
                          range_key_type='N'):
        attrs = {}
        attrs[partition_key_name] = partition_key_type
        key_schema = [(partition_key_name, 'HASH')]
        if range_key_name is not None:
            attrs[range_key_name] = range_key_type
            key_schema.append((range_key_name, 'RANGE'))
        return {
            "key_schema": OrderedDict(key_schema),
            "attribute_definitions": attrs
        }

    def default_dynamo_tables(self, context):
        """
        Create default DynamoDB tables
        """
        source_state_config = ResourceManager.dynamo_key_schema("source_config_hash")
        source_state_table = r.DynamoDBTableResource(
            context, self.dynamo_table_name("source_state"),
            attribute_definitions=source_state_config['attribute_definitions'],
            key_schema=source_state_config['key_schema'],
            write_units=20, read_units=20
        )

        source_list_config = ResourceManager.dynamo_key_schema("uuid")
        source_list_table = r.DynamoDBTableResource(
            context, self.dynamo_table_name("source_list"),
            attribute_definitions=source_list_config['attribute_definitions'],
            key_schema=source_list_config['key_schema'],
            write_units=20, read_units=20
        )

        return [
            source_state_table,
            source_list_table
        ]

    def create_resource_cluster(self):
        """
        Create a RedLeader cluster for AWS resource creation
        """
        context = AWSContext(self._controller._aws_profile)
        cluster = Cluster(self._cluster_name(), context)

        # Create an SQS queue for each unique item type
        queues = {}
        for item_type in self._controller.item_types():
            dead_letter_queue = r.SQSQueueResource(context,
                                                   self.dead_letter_queue_name(item_type),
                                                   is_static=False)
            queues[item_type] = r.SQSQueueResource(context,
                                                   self.queue_name(item_type),
                                                   dead_letter_queue=dead_letter_queue,
                                                   dead_letter_queue_retries=3,
                                                   is_static=False)
            cluster.add_resource(dead_letter_queue)
            cluster.add_resource(queues[item_type])

        # Create a ref to cloudwatch logs so we can create an appropriate role
        logs = r.CloudWatchLogs(context)
        cluster.add_resource(logs)

        # Create a reference to our configuration bucket
        bucket = r.S3BucketResource(context, self._controller.config_bucket_name())
        cluster.add_resource(bucket)

        # Incorporate resources needed by individual sources & transformers
        resources = self.aws_resources_needed() + \
                    self.default_dynamo_tables(context) + \
                    [bucket]
        for resource in resources:
            cluster.add_resource(resource)

        # Create a role for lambda jobs
        permissions = []
        for item_type in queues:
            permissions.append(r.ReadWritePermission(queues[item_type]))
        for resource in resources:
            permissions.append(r.ReadWritePermission(resource))
        permissions.append(r.ReadWritePermission(logs))

        self.lambdaRole = r.IAMRoleResource(
            context,
            permissions=permissions,
            services=["lambda.amazonaws.com", "events.amazonaws.com"],
            policy_arns=["arn:aws:iam::aws:policy/AWSLambdaFullAccess",
                         "arn:aws:iam::aws:policy/CloudWatchEventsFullAccess",
                         "arn:aws:iam::aws:policy/CloudWatchFullAccess"
            ])
        cluster.add_resource(self.lambdaRole)
        #print(json.dumps(cluster.cloud_formation_template(), indent=4))

        return cluster
