"""

The Source class defines a single interface for gathering information
from any information source.

Sources will be used as such:
```
source = MySource({parameter: value})
for item in source.yield_items():
    # Do something with item
    pass
```

Sources are designed to be interruptible. In order to permit this, sources
can save their state and resume from that state.

"""

class Source(object):
    def __init__(self, params, prev_state):
        # Validate given parameters
        self._validate_params(params)

        # Attach parameters to this object
        self.params = params

    def save_state(self, state):
        #TODO: Store in DynamoDB with key: self.__class__.__name__ + hash(params)
        # and value json.dumps(state)
        raise NotImplementedError

    def resume_from_state(self):
        raise NotImplementedError

    def yield_items(self):
        raise NotImplementedError

class StaticFileSource(object):
    pass

class PaginatedAPISource(object):
    def __init__(self, params, prev_state):
        pass
