import warnings

from ...cluster import MAX_RESULT_WINDOW


class BaseSearchQueryWrapper(object):
    """Elasticsearch also returns total hits count with search response.
    Thus we can get documents and total hits making single request.
    """
    def __init__(self, query, max_items=MAX_RESULT_WINDOW):
        self.query = query
        self.max_items = max_items
        self.sliced_query = None
        self.items = None
        self.count = None

    def _prepare_getitem(self, k):
        if not isinstance(k, slice):
            raise ValueError('__getitem__ without slicing is not supported')
        if k.start is not None:
            start = min(k.start, self.max_items)
        else:
            start = None
        if k.stop is not None:
            stop = min(k.stop, self.max_items)
        else:
            stop = None
        self.sliced_query = self.query.slice(start, stop)


class SearchQueryWrapper(BaseSearchQueryWrapper):
    def __getitem__(self, k):
        self._prepare_getitem(k)
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
        """Deprecated!!!
        """
        warnings.warn(
            'Field `result` is deprecated, use `get_result` method instead',
            DeprecationWarning
        )
        return self.get_result()

    @property
    def results(self):
        """Deprecated!!!
        """
        warnings.warn(
            'Field `results` is deprecated, use `get_result` method instead',
            DeprecationWarning
        )
        return self.get_result()
