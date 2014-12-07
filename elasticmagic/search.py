from itertools import chain

from .util import _with_clone, cached_property
from .result import Result
from .expression import Expression, Params, Filtered, And, Bool, FunctionScore, Compiled


__all__ = ['SearchQuery']


class Source(Expression):
    __visit_name__ = 'source'

    def __init__(self, fields, include=None, exclude=None):
        self.fields = fields
        self.include = include
        self.exclude = exclude


class Rescore(Expression):
    __visit_name__ = 'rescore'

    def __init__(self, query, window_size=None,
                 query_weight=None, rescore_query_weight=None, score_mode=None):
        self.query = query
        self.window_size = window_size
        self.query_weight = query_weight
        self.rescore_query_weight = rescore_query_weight
        self.score_mode = score_mode


class SearchQuery(object):
    __visit_name__ = 'search_query'

    _q = None
    _source = None
    _filters = ()
    _post_filters = ()
    _order_by = ()
    _aggregations = Params()
    _boost_functions = ()
    _boost_params = Params()
    _limit = None
    _offset = None
    _rescores = ()

    _cluster = None
    _index = None
    _doc_cls = None
    _doc_type = None
    _routing = None
    _search_type = None

    _instance_mapper = None
    _iter_instances = False

    def __init__(self, q=None, 
                 cluster=None, index=None,
                 doc_cls=None, doc_type=None,
                 routing=None, search_type=None):
        if q is not None:
            self._q = q
        if cluster:
            self._cluster = cluster
        if index:
            self._index = index
        if doc_cls:
            self._doc_cls = doc_cls
        if doc_type:
            self._doc_type = doc_type
        if routing:
            self._routing = routing
        if search_type:
            self._search_type = search_type

    def clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = {k: v for k, v in self.__dict__.items()
                      if not isinstance(getattr(cls, k, None), cached_property)}
        return q

    def to_dict(self):
        return Compiled(self).params

    @_with_clone
    def source(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            del self._source
        elif len(args) == 1 and args[0] is False:
            self._source = Source(args[0], **kwargs)
        else:
            self._source = Source(args, **kwargs)

    fields = source

    @_with_clone
    def add_fields(self, *fields):
        self._source = self._source + fields

    @_with_clone
    def filter(self, *filters, **kwargs):
        meta = kwargs.pop('meta', None)
        self._filters = self._filters + ((filters, meta),)

    @_with_clone
    def post_filter(self, *filters, **kwargs):
        meta = kwargs.pop('meta', None)
        self._post_filters = self._post_filters + ((filters, meta),)

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
    def boost_function(self, *args, **kwargs):
        if args == (None,):
            del self._boost_functions
            del self._boost_params
        else:
            self._boost_functions = self._boost_functions + args
            self._boost_params = Params(dict(self._boost_params), **kwargs)

    bf = boost_function

    @_with_clone
    def limit(self, limit):
        self._limit = limit

    size = limit

    @_with_clone
    def offset(self, offset):
        self._offset = offset

    from_ = offset

    @_with_clone
    def rescore(self, query, window_size=None,
                query_weight=None, rescore_query_weight=None, score_mode=None):
        if query is None:
            del self._rescores
            return
        rescore = Rescore(
            query, window_size=window_size, query_weight=query_weight,
            rescore_query_weight=rescore_query_weight, score_mode=score_mode,
        )
        self._rescores = self._rescores + (rescore,)

    @_with_clone
    def instances(self):
        self._iter_instances = True

    @_with_clone
    def with_cluster(self, cluster):
        self._cluster = cluster

    @_with_clone
    def with_index(self, index):
        self._index = index

    @_with_clone
    def with_document(self, doc_cls):
        self._doc_cls = doc_cls

    @_with_clone
    def with_doc_type(self, doc_type):
        self._doc_type = doc_type

    @_with_clone
    def with_instance_mapper(self, instance_mapper):
        self._instance_mapper = instance_mapper

    @_with_clone
    def with_routing(self, routing):
        self._routing = routing

    @_with_clone
    def with_search_type(self, search_type):
        self._search_type = search_type

    def _get_doc_cls(self):
        if self._doc_cls:
            doc_classes = [self._doc_cls]
        else:
            doc_classes = self._collect_doc_classes()

        if len(doc_classes) != 1:
            raise ValueError('Cannot determine document class')

        return iter(doc_classes).next()

    def _get_doc_type(self, doc_cls=None):
        doc_cls = doc_cls or self._get_doc_cls()
        if isinstance(doc_cls, tuple):
            return ','.join(d.__doc_type__ for d in doc_cls)
        else:
            return self._doc_type or doc_cls.__doc_type__

    def get_query(self, wrap_function_score=True):
        if wrap_function_score and self._boost_functions:
            return FunctionScore(
                query=self._q,
                functions=self._boost_functions,
                **self._boost_params
            )
        return self._q

    def get_filtered_query(self, wrap_function_score=True):
        q = self.get_query(wrap_function_score=wrap_function_score)
        if self._filters:
            return Filtered(query=q, filter=Bool.must(*self.iter_filters()))
        return q

    def iter_filters_with_meta(self):
        for filters, meta in self._filters:
            for f in filters:
                yield f, meta

    def iter_filters(self):
        return (f for f, m in self.iter_filters_with_meta())

    def iter_post_filters_with_meta(self):
        for filters, meta in self._post_filters:
            for f in filters:
                yield f, meta

    def iter_post_filters(self):
        return (f for f, m in self.iter_post_filters_with_meta())

    @cached_property
    def result(self):
        doc_cls = self._get_doc_cls()
        doc_type = self._get_doc_type(doc_cls)
        return (self._index or self._cluster).search(
            self,
            doc_type=doc_type,
            routing=self._routing,
            search_type=self._search_type,
        )

    @property
    def results(self):
        return self.result

    def count(self):
        return self._index.count(
            self.get_filtered_query(wrap_function_score=False),
            doc_type=self._get_doc_type(),
            routing=self._routing,
        )

    def exists(self, refresh=None):
        return self._index.exists(
            self.get_filtered_query(wrap_function_score=False),
            self._get_doc_type(),
            refresh=refresh,
            routing=self._routing,
        )

    def delete(self, timeout=None, consistency=None, replication=None):
        return self._index.delete_by_query(
            self.get_filtered_query(wrap_function_score=False),
            self._get_doc_type(),
            timeout=timeout,
            consistency=consistency,
            replication=replication,
        )

    def _collect_doc_classes(self):
        doc_types = set()
        for expr in chain([self._q],
                          [self._source],
                          chain(*[f for f, m in self._filters]),
                          chain(*[f for f, m in self._post_filters]),
                          self._order_by,
                          self._aggregations.values()):
            if expr and hasattr(expr, '_collect_doc_classes'):
                doc_types.update(expr._collect_doc_classes())
        return doc_types

    def __iter__(self):
        if self._iter_instances:
            return iter(doc.instance for doc in self.result.hits if doc.instance)
        return iter(self.result)

    def __len__(self):
        return len(self.result.hits)

    def __getitem__(self, k):
        if not isinstance(k, (slice, int)):
            raise TypeError

        if 'results' in self.__dict__:
            docs = self.result.hits[k]
        else:
            if isinstance(k, slice):
                start, stop = k.start, k.stop
                clone = self.clone()
                if start is not None:
                    clone._offset = start
                if stop is not None:
                    if start is None:
                        clone._limit = stop
                    else:
                        clone._limit = stop - start
                return clone
            else:
                docs = self.result.hits[k]
        if self._iter_instances:
            return [doc.instance for doc in docs if doc.instance]
        return docs
