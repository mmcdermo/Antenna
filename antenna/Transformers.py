# Copyright 2016 Morgan McDermott & Blake Allen
"""
Transformers consume Items produced by Sources and other Transformers,
in turn producing new Items themselves.

Transformers are not designed to be interruptible - they are expected
to finish within the 5 minute execution time limit imposed by AWS Lambda.
"""
from functools import reduce
import requests

from antenna.Sources import Item

class Item(object):
    def __init__(self, item_type="", payload={}):
        self.item_type = item_type
        self.payload = payload

class Transformer(object):
    def __init__(self, aws_manager, params):
        # Validate given parameters
        self._meta_keywords = ["type", "storage", "filters"]
        self._validate_params(params)

        # Attach parameters to this object
        self.params = params
        for param in params:
            setattr(self, param, params[param])

        self._aws_manager = aws_manager

    def _validate_params(self, params):
        for param in self._required_keywords:
            if param not in params:
                raise Exception("Missing parameter %s for transformer %s" %
                                (param, self.__class__.__name__))
        for param in params:
            if param not in self._meta_keywords and \
               param not in self._required_keywords and \
               (not hasattr(self, "_optional_keywords") or \
                param not in self._optional_keywords):
                raise Exception("Unknown parameter `%s` for transformer %s" %
                                (param, self.__class__.__name__))

    def input_item_types(self):
        """
        Defines the item types that this transformer consumes.
        E.g.) return ["article", "blogpost"]
        """
        if hasattr(self, "input_item_types"):
            return self.input_item_types
        raise NotImplementedError

    def output_item_types(self):
        """
        Defines the output item types that this transformer produces.
        E.g.) return ["processed_article"]
        """
        if hasattr(self, "output_item_types"):
            return self.output_item_types
        raise NotImplementedError

    def external_resources(self):
        """
        Returns a list of RedLeader.resources this transformer requires access to
        """
        return []

    def transform_items(self, items):
        """
        By default, transformers map over consumed items. However, a transformer
        can produce more or less items than it consumes by overriding this method
        """
        for item in items:
            yield self.transform(item)

    def transform(self, item):
        """
        Implemented by child classes
        """
        raise NotImplementedError

class ScrapePageTransformer(Transformer):
    """
    Consumes ArticleReference Items
    Side Effects: Stores article bodies on S3
    Produces: ScrapedArticle Items
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
        ]
        super(ArticleScrapeTransformer, self).__init__(aws_manager, params)

    def transform(self, item):
        res = requests.get(item['url'])
        item["scraped_page"] = json.dumps(res)
        return item

class ArticleAnalysisTransformer(Transformer):
    """
    Consumes ScrapedArticle Items
    Side Effects: Creates DynamoDB Article Reference
    Produces: None
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "dynamodb_table_name",
            "machine_learning_api_endpoint"
        ]
        super(ArticleAnalysisTransformer, self).__init__(aws_manager, params)

    def transform(self, item):
        item['score'] = 4.2
        return item



        pass
        #TODO

class DynamoDBStorage(Transformer):
    """
    Consumes ScrapedArticle Items
    Side Effects: Creates DynamoDB Article Reference
    Produces: None
    """
    def input_item_types(self):
        return ["ArticleReference"]

    def output_item_types(self):
        return []

    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "dynamodb_table_name"
        ]
        super(DynamoDBStorage, self).__init__(aws_manager, params)

    def transform(self, item):
        ddb = self._aws_manager.get_client('dynamodb')
        ddb.put_item(
            TableName=self.dynamodb_table_name,
            Item={
                "article_url_key": {'S': item.payload["url"]},
                "article_title": {'S': item.payload["title"]},
                "article_published": {'N': str(item.payload["time"])},
                "article_summary": {'S': item.payload["summary"]}
            })

class IdentityTransformer(Transformer):
    """
    Consumes Items of given type
    Side Effects: None
    Produces: Items of the given type
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "input_item_types",
            "output_item_types"
        ]
        super(IdentityTransformer, self).__init__(aws_manager, params)

    def transform(self, item):
        return Item(item_type=self.input_item_types[0],
                     payload=item.payload)
