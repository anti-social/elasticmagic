from itertools import chain

from .util import _with_clone, cached_property
from .result import Result
from .expression import Params, Compiled


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

    _instance_mapper = None

    def __init__(self, q=None, index=None, doc_cls=None, doc_type=None):
        if q is not None:
            self._q = q
        self.index = index
        self.doc_cls = doc_cls
        self.doc_type = doc_type

    def clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = {k: v for k, v in self.__dict__.items()
                      if not isinstance(getattr(cls, k, None), cached_property)}
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
    def filter(self, *filters, **meta):
        if len(filters) > 1:
            f = (And(*filters), meta)
        else:
            f = (filters[0], meta)
        self._filters = self._filters + (f,)

    @_with_clone
    def order_by(self, *orders):
        if len(orders) == 1 and orders[0] is None:
            if '_order_by' in self.__dict__:
                del self._order_by
        else:
            self._order_by = self._order_by + orders

    @_with_clone
    def aggregations(self, *args, **aggs):
        if len(args) == 1 and args[0] is None:
            if '_aggregations' in self.__dict__:
                del self._aggregations
        if aggs:
            self._aggregations = Params(dict(self._aggregations), **aggs)

    aggs = aggregations

    @_with_clone
    def limit(self, limit):
        self._limit = limit

    size = limit

    @_with_clone
    def offset(self, offset):
        self._offset = offset

    from_ = offset

    @_with_clone
    def with_index(self, index):
        self.index = index

    @_with_clone
    def with_document(self, doc_cls):
        self.doc_cls = doc_cls

    @_with_clone
    def with_doc_type(self, doc_type):
        self.doc_type = doc_type

    @_with_clone
    def with_instance_mapper(self, instance_mapper):
        self._instance_mapper = instance_mapper

    def count(self):
        sq = self.aggregation(None).order_by(None).limit(0)
        return sq.results.total

    @cached_property
    def results(self):
        client = self.index._client
        if self.doc_cls:
            doc_classes = [self.doc_cls]
        else:
            doc_classes = self._collect_doc_classes()
        if len(doc_classes) != 1:
            raise ValueError('Cannot determine document type')

        doc_cls = doc_classes.pop()
        doc_type = self.doc_type or doc_cls.__doc_type__
        raw_result = client.search(index=self.index._name,
                                   doc_type=doc_type,
                                   body=self.to_dict())
        return Result(raw_result, self._aggregations,
                      doc_cls=doc_cls,
                      instance_mapper=self._instance_mapper)

    def _collect_doc_classes(self):
        doc_types = set()
        for expr in chain([self._q],
                          self._fields,
                          [f for f, m in self._filters],
                          self._order_by,
                          self._aggregations.values()):
            if expr and hasattr(expr, '_collect_doc_classes'):
                doc_types.update(expr._collect_doc_classes())
        return doc_types

    def __iter__(self):
        return iter(self.results)


# es_client = Elasticsearch()
# es_index = Index(es_client, 'uaprom')

# ## 1
# class ProductDocument(Document):
#     __doc_type__ = 'product'

#     name = Field(String)

# es_index.search().with_doc_type(ProductDocument)
# es_index.search(ProductDocument.name.match('iphone 5'))

# ## 2.1
# class ProductDocument(Document):
#     name = Field(String)

# es_index.product.search()
# es_index.product.search(ProductDocument.name.match('iphone 5'))

# ## 2.2
# es_index.search(doc_type='product')
# es_index.search(ProductDocument.name.match('iphone 5'), doc_type='product')
