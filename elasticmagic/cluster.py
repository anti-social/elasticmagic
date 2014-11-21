from .index import Index


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

    # def multi_search(self):
    #     pass

    # msearch = multi_search
