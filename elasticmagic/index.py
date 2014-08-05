from .search import SearchQuery


class Index(object):
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def search(self, *args, **kwargs):
        kwargs['index'] = self
        return SearchQuery(*args, **kwargs)
