class Monitor(object):
    def __init__(self, aws_manager):
        self._aws_manager = aws_manager

    def put_metric(self, dimensions, value):
        pass
