# Copyright 2016 Morgan McDermott & Blake Allen
"""
The Source class defines a single interface for gathering information,
either from live/changing sources or static archives.

Sources produce items with yield_items()
```
source = MySource({parameter: value})
for item in source.yield_items():
    # Do something with item
    pass
```

Sources are designed to be interruptible. In order to permit this, sources
can save their state and resume from that state. This makes the service
fault tolerant, and also allows us to execute long-running scrapes
on large archives.

Source state persistence to DynamoDB is managed by the Controller
"""
import requests
import boto3
import feedparser
import hashlib
import json
import time
import newspaper
import calendar

from urllib.parse import urlparse

class Item(object):
    def __init__(self, item_type="", payload={}):
        self.item_type = item_type
        self.payload = payload


class Source(object):
    def __init__(self, aws_manager, params):
        # Validate provided parameters
        self._aws_manager = aws_manager
        self._validate_params(params)

        # Attach parameters to this object
        self.params = params
        for param in params:
            setattr(self, param, params[param])

        # Attach default values to this object if defined
        if hasattr(self, "_defaults"):
            for key in self._defaults:
                if key not in params:
                    setattr(self, key, self._defaults[key])

        if not hasattr(self, "state"):
            self.state = {}

    def external_resources(self):
        """
        Returns a list of RedLeader.resources this source requires access to
        """
        return []

    def _validate_params(self, obj):
        for keyword in self._required_keywords:
            if keyword not in obj:
                raise Exception("Invalid config for class %s: Missing keyword %s" %
                                (self.__class__.__name__, keyword))

    def has_new_data(self):
        """
        Returns a boolean value indicating whether the source has new data and
        needs to run, given the provided parameters.

        Example: A static file source doesn't need to run if the static
        file already exists on S3.
        """
        return True

    def config_hash(self):
        """
        Return unique key for this source's current state.

        Returns: self.__class__.__name__ + hash(params)
        """
        h = hashlib.md5()
        param_json = json.dumps(self.params, sort_keys=True)
        h.update(str(param_json).encode('utf-8'))
        return self.__class__.__name__ + str(h.hexdigest())

    def set_state(self, state):
        if state is None:
            return
        print("Setting state", state)
        for k in state:
            self.state[k] = state[k]

    def get_state(self):
        return self.state

    def yield_items(self):
        """
        Implemented by each source individually
        """
        raise NotImplementedError


class StaticFileSource(Source):
    """
    Retrieves files from the web and stores them on S3.
    Side Effects: Stores files on S3
    Produces: Items of whatever type is specified in params,
              or no items if unspecified.
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            's3_bucket_name',
            'source_url',
            'destination_key'
        ]
        self._defaults = {
            'item_type': None # By default, produce no items
        }
        super(StaticFileSource, self).__init__(aws_manager, params)

    def has_new_data(self):
        s3_client = self._aws_manager.get_client('s3')
        objects = s3_client.list_objects(Bucket=self.s3_bucket_name,
                                         Prefix=self.destination_key)
        return 'Contents' not in objects or len(objects['Contents']) == 0

    def yield_items(self):
        s3_client = self._aws_manager.get_client('s3')
        local_filename = self.s3_bucket_name + "_" + self.destination_key
        r = requests.get(self.source_url, stream=True)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        s3_client.upload_file(local_filename, self.s3_bucket_name, self.destination_key)
        yield Item(item_type=self.item_type, payload=self.params)


class RSSFeedSource(Source):
    """
    Scrapes RSS Feeds for a given site
    Side Effects: None
    Produces: ArticleReference Items

    TODO: Store last retrieved article date in state, so we can
          easily decide whether or not to run.
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "rss_feed_url"
        ]
        self._optional_keywords = [
            "minutes_between_scrapes",
            "keywords"
        ]
        self._defaults = {
            'item_type': 'ArticleReference',
            'minutes_between_scrapes': 10,
        }
        self.state = {
            "time_last_updated": 0
        }
        super(RSSFeedSource, self).__init__(aws_manager, params)

    def has_new_data(self):
        # Only scrape if it's been at least 10 minutes since the
        # last article was seen
        print("RSS Feed last run at %s" % self.state['time_last_updated'])
        return time.time() - float(self.state['time_last_updated']) > 60 * self.minutes_between_scrapes

    def get_keywords(self):
        if hasattr(self, "keywords"):
            return self.keywords
        else:
            return []

    def yield_items(self):
        self.state = {
            'time_last_updated': time.time()
        }
        feed = feedparser.parse(self.rss_feed_url)
        for entry in feed['entries']:
            timestamp = calendar.timegm(entry['published_parsed'])
            content = entry['summary']
            try:
                content = entry['content'][0]['value']
            except Exception as e:
                pass
            yield Item(item_type=self.item_type,
                       payload={
                           'title': entry['title'],
                           'url': entry['link'],
                           'content': content,
                           'source_type': 'RSS',
                           'source_keywords': self.get_keywords(),
                           'time_sourced': time.time(),
                           'domain': urlparse(self.rss_feed_url).netloc,
                           'source_url': self.rss_feed_url,
                           'time_published': timestamp,
                           'summary': entry['summary']
                       })


class NewspaperLibSource(Source):
    """
    Consumes ArticleReference Items
    Side Effects: Stores article bodies on S3
    Produces: ScrapedArticle Items
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "url",
            "item_type"
        ]
        self._defaults = {
            'item_type': 'ArticleReference',
            'minutes_between_scrapes': 10,
        }
        self._optional_keywords = [
            "minutes_between_scrapes"
        ]
        self.state = {
            "time_last_updated": 0
        }
        super(NewspaperLibSource, self).__init__(aws_manager, params)

    def has_new_data(self):
        # Only scrape if it's been at least 10 minutes since the
        # last article was seen
        print("RSS Feed last run at %s" % self.state['time_last_updated'])
        return time.time() - float(self.state['time_last_updated']) > 60 * self.minutes_between_scrapes

    def yield_items(self):
        self.state = {
            'time_last_updated': time.time()
        }
        print("Building newspaper lib source for URL %s" % self.url)
        source = newspaper.build(self.url, memoize_articles=False)
        print("Finished building newspaper lib source. Found %d articles" % source.size())
        for a in source.articles:
            payload = {
                'url': a.url,
                'source_type': 'NewspaperLib',
                'time_sourced': time.time(),
                'domain': urlparse(self.url).netloc,
                'source_url': self.url
            }
            yield Item(
                item_type=self.item_type,
                payload=payload)

class ArchivedRedditSubmissionsSource(Source):
    """
    Acquires archived reddit submissions (in the format published by pushshift.io)
    Side Effects: Stores subreddit specific CSVs, Creates DynamoDB Social Media Refs
    Produces: ArticleReference Items

    TODO: Implement this source
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "s3_bucket_name",
            "s3_key_prefix"
        ]
        super(StaticFileSource, self).__init__(aws_manager, params)


class PaginatedAPISource(object):
    """
    Flexibly scrape an API given a configuration like:
    {
        'api_url': 'https://somewebsite/api/v1/list_news',
        'api_key_url_parameter': 'api-key',
        'api_key_url_value': 'nrbg5lb73jk45dky195ntndfx9',
        'next_page_url_parameter': 'next',
        'next_page_path': 'results.meta.next_page_id'
        'items_path': 'results.items',
        'url_path': 'article_info.url',
        'title_path': 'article_info.article_name',
    }

    Side Effects: none
    Produces: ArticleReference Items

    TODO: Implement this Source
    """
    def __init__(self, params):
        super().__init__(params)
        self._required_keywords= []
