# Copyright 2016 Morgan McDermott & Blake Allen
"""
Transformers consume Items produced by Sources and other Transformers,
in turn producing new Items themselves.

Transformers are not designed to be interruptible - they are expected
to finish within the 5 minute execution time limit imposed by AWS Lambda.
"""
from functools import reduce
from newspaper import Article
#from readability import Document
import time
import calendar
import collections
import datefinder
import datetime
import dateutil.parser as dparser
import requests


from antenna.Sources import Item

class Item(object):
    def __init__(self, item_type="", payload={}):
        self.item_type = item_type
        self.payload = payload

class Transformer(object):
    def __init__(self, aws_manager, params):
        # Validate given parameters
        self._meta_keywords = ["type", "storage", "filters",
                               "input_item_types", "output_item_types"]
        self._validate_params(params)

        # Attach parameters to this object
        self.params = params
        for param in params:
            setattr(self, param, params[param])

        self._aws_manager = aws_manager
        self.input_item_types = getattr(self, "input_item_types",
                                        self.params.get("input_item_types", []))
        self.output_item_types = getattr(self, "output_item_types",
                                         self.params.get("output_item_types", []))

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

class DirtyScraper(Transformer):
    """
    Scrape all URLs on page
    TODO: Implement
    """
    pass

class URLScraper(Transformer):
    """
    Accept CSS selectors, regexes
    TODO: Implement
    """
    pass

def date_extraction_helper(content):
    """
    Searches `content` for the most common mentioned date within 10 years of now
    """
    matches = list(datefinder.find_dates(content))

    now = calendar.timegm(datetime.datetime.now().timetuple())
    timestamps = map(lambda x: calendar.timegm(x.timetuple()), matches)
    filtered = filter(lambda x: abs(x - now) < 60 * 60 * 24 * 365 * 10, timestamps)

    most_common = None
    hwm = 0
    counts = collections.defaultdict(lambda: 0)
    for timestamp in filtered:
        counts[timestamp] += 1
        if counts[timestamp] > hwm:
            most_common = timestamp
            hwm = counts[timestamp]

    print("Most referenced date: %s" % datetime.datetime.utcfromtimestamp(most_common))
    return most_common

class NewspaperLibScraper(Transformer):
    """
    Input item payloads should have shape {'url': 'http://...', ...}
    Output items will be augmented with title, fulltext, images, authors, etc
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
            "input_item_types",
            "output_item_type"
        ]
        super(NewspaperLibScraper, self).__init__(aws_manager, params)
        self.output_item_types = [self.output_item_type]

    def transform(self, item):
        url = item.payload['url']
        print("NewspaperLibScraper scraping URL %s" % url)
        a = Article(url, language='en')
        a.download()
        a.parse()


        # Extract date using readability, since
        # newspaper's date extraction is unreliable
        #doc = Document(a.html)

        item.payload['title'] = a.title
        item.payload['fulltext'] = a.text
        item.payload['image'] = a.top_image
        item.payload['images'] = list(a.images)
        item.payload['movies'] = list(a.movies)
        item.payload['authors'] = list(a.authors)
        item.payload['scrape_time'] = time.time()
        if a.publish_date is not None:
            item.payload['time_published'] = calendar.timegm(a.publish_date.timetuple())
            week = datetime.date.fromtimestamp(item.payload['time_published']).isocalendar()
            item.payload['week_published'] = "%s_%s" % (week[0], week[1])
            print("Date from newspaperlib: %s" % item.payload['time_published'])
        else:
            item.payload['time_published'] = date_extraction_helper(a.html)
            print("Date from helper: %s" % item.payload['time_published'])
        return Item(
            item_type=self.output_item_type,
            payload=item.payload)

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
