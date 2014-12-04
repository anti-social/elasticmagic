from .util import clean_params
from .index import Index
from .helpers import multi_search as _multi_search


class Cluster(object):
    def __init__(self, client):
        self._client = client

        self._index_cache = {}

    def __getitem__(self, index_name):
        return self.get_index(index_name)

    def get_index(self, name):
        if isinstance(name, tuple):
            name = ','.join(name)

        if name not in self._index_cache:
            self._index_cache[name] = Index(self._client, name)
        return self._index_cache[name]

    def multi_search(self, *queries, **params):
        return _multi_search(self._client, queries, params)

    msearch = multi_search
