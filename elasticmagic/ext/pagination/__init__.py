import logging

log = logging.getLogger(__name__)


class SearchQueryWrapper(object):
    """Elasticsearch returns total count with response.
    So we can get documents and count with one request.
    """
    def __init__(self, query):
        self.query = query
        self.sliced_query = None
        self.items = None
        self.count = None

    def __getitem__(self, range):
        if not isinstance(range, slice):
            raise ValueError('__getitem__ without slicing not supported')
        self.sliced_query = self.query[range]
        self.items = list(self.sliced_query)
        self.count = self.sliced_query.get_result().total
        return self.items

    def __iter__(self):
        if self.items is None:
            raise ValueError('Slice first')
        return iter(self.items)

    def __len__(self):
        if self.count is None:
            raise ValueError('Slice first')
        return self.count

    def get_result(self):
        if self.sliced_query is None:
            raise ValueError('Slice first')
        return self.sliced_query.get_result()

    @property
    def result(self):
        log.warning('Field "result" is deprecated')
        return self.get_result()

    @property
    def results(self):
        log.warning('Field "results" is deprecated')
        return self.get_result()
