from .expression import Params, Compiled
from .util import _with_clone


__all__ = ['SearchQuery']


class SearchQuery(object):
    __visit_name__ = 'search_query'

    _q = None
    _fields = ()
    _filters = ()
    _order_by = ()
    _aggregations = Params()
    _limit = None
    _offset = None

    def __init__(self, q=None):
        if q is not None:
            self._q = q

    def clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = self.__dict__.copy()
        return q

    def to_dict(self):
        return Compiled(self).params

    @_with_clone
    def fields(self, *fields):
        self._fields = fields

    @_with_clone
    def add_fields(self, *fields):
        self._fields = self._fields + fields

    @_with_clone
    def filter(self, *filters):
        self._filters = self._filters + filters

    @_with_clone
    def order_by(self, *orders):
        if len(orders) == 1 and orders[0] is None:
            del self._order_by
        else:
            self._order_by = self._order_by + orders

    @_with_clone
    def aggregation(self, **aggs):
        self._aggregations = Params(dict(self._aggregations), **aggs)

    agg = aggregation

    @_with_clone
    def limit(self, limit):
        self._limit = limit

    size = limit

    @_with_clone
    def offset(self, offset):
        self._offset = offset

    from_ = offset

    def count(self):
        pass
