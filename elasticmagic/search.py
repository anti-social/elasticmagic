import warnings
import collections

from .compat import zip
from .util import _with_clone, cached_property, clean_params, merge_params, collect_doc_classes
from .result import Result
from .compiler import DefaultCompiler
from .expression import Expression, QueryExpression, Params, Filtered, And, Bool, FunctionScore


__all__ = ['SearchQuery']


class Source(Expression):
    __visit_name__ = 'source'

    def __init__(self, fields, include=None, exclude=None):
        self.fields = fields
        self.include = include
        self.exclude = exclude

    def _collect_doc_classes(self):
        return set().union(
            collect_doc_classes(self.fields),
            collect_doc_classes(self.include),
            collect_doc_classes(self.exclude),
        )


class QueryRescorer(QueryExpression):
    __visit_name__ = 'query_rescorer'

    def __init__(self, rescore_query, query_weight=None, rescore_query_weight=None, score_mode=None, **kwargs):
        super(QueryRescorer, self).__init__(
            rescore_query=rescore_query, query_weight=query_weight,
            rescore_query_weight=rescore_query_weight, score_mode=score_mode,
            **kwargs
        )


class Rescore(Expression):
    __visit_name__ = 'rescore'

    def __init__(self, rescorer, window_size=None,
                 ):
        self.rescorer = rescorer
        self.window_size = window_size

    def _collect_doc_classes(self):
        return collect_doc_classes(self.rescorer)


class SearchQuery(object):
    __visit_name__ = 'search_query'

    _q = None
    _source = None
    _filters = ()
    _filters_meta = ()
    _post_filters = ()
    _post_filters_meta = ()
    _order_by = ()
    _aggregations = Params()
    _function_score = ()
    _function_score_params = Params()
    _limit = None
    _offset = None
    _rescores = ()
    _suggest = Params()

    _cluster = None
    _index = None
    _doc_cls = None
    _doc_type = None

    _search_params = Params()

    _instance_mapper = None
    _iter_instances = False

    def __init__(
            self, q=None,
            cluster=None, index=None, doc_cls=None, doc_type=None,
            routing=None, preference=None, timeout=None, search_type=None,
            query_cache=None, terminate_after=None, scroll=None,
            _compiler=None, **kwargs
    ):
        self._compiler = _compiler or DefaultCompiler().get_query_compiler()

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

        search_params = Params(
            routing=routing,
            preference=preference,
            timeout=timeout,
            search_type=search_type,
            query_cache=query_cache,
            terminate_after=terminate_after,
            scroll=scroll,
            **kwargs
        )
        if search_params:
            self._search_params = search_params

    def clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = {k: v for k, v in self.__dict__.items()
                      if not isinstance(getattr(cls, k, None), cached_property)}
        return q

    def to_dict(self):
        return self._compiler(self).params

    @_with_clone
    def source(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            if '_source' in self.__dict__:
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
    def query(self, q):
        if q is None:
            if '_q' in self.__dict__:
                del self._q
        else:
            self._q = q
        
    @_with_clone
    def filter(self, *filters, **kwargs):
        meta = kwargs.pop('meta', None)
        self._filters = self._filters + filters
        self._filters_meta = self._filters_meta + (meta,) * len(filters)

    @_with_clone
    def post_filter(self, *filters, **kwargs):
        meta = kwargs.pop('meta', None)
        self._post_filters = self._post_filters + filters
        self._post_filters_meta = self._post_filters_meta + (meta,) * len(filters)

    @_with_clone
    def order_by(self, *orders):
        if len(orders) == 1 and orders[0] is None:
            if '_order_by' in self.__dict__:
                del self._order_by
        else:
            self._order_by = self._order_by + orders

    @_with_clone
    def aggregations(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            if '_aggregations' in self.__dict__:
                del self._aggregations
        else:
            self._aggregations = merge_params(self._aggregations, args, kwargs)

    aggs = aggregations

    @_with_clone
    def function_score(self, *args, **kwargs):
        if args == (None,):
            if '_function_score' in self.__dict__:
                del self._function_score
                del self._function_score_params
        else:
            self._function_score = self._function_score + args
            self._function_score_params = Params(dict(self._function_score_params), **kwargs)

    @_with_clone
    def limit(self, limit):
        self._limit = limit

    size = limit

    @_with_clone
    def offset(self, offset):
        self._offset = offset

    from_ = offset

    @_with_clone
    def rescore(self, rescorer, window_size=None):
        if rescorer is None:
            if '_rescores' in self.__dict__:
                del self._rescores
            return
        rescore = Rescore(rescorer, window_size=window_size)
        self._rescores = self._rescores + (rescore,)

    @_with_clone
    def suggest(self, *args, **kwargs):
        if args == (None,):
            if'_suggest' in self.__dict__:
                del self._suggest
        else:
            self._suggest = merge_params(self._suggest, args, kwargs)

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

    def with_routing(self, routing):
        return self.with_search_params(routing=routing)

    def with_preference(self, preference):
        return self.with_search_params(preference=preference)

    def with_timeout(self, timeout):
        return self.with_search_params(timeout=timeout)

    def with_search_type(self, search_type):
        return self.with_search_params(search_type=search_type)

    def with_query_cache(self, query_cache):
        return self.with_search_params(query_cache=query_cache)

    def with_terminate_after(self, terminate_after):
        return self.with_search_params(terminate_after=terminate_after)

    def with_scroll(self, scroll):
        return self.with_search_params(scroll=scroll)

    @_with_clone
    def with_search_params(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            if '_search_params' in self.__dict__:
                del self._search_params
        elif args or kwargs:
            search_params = Params(self._search_params, *args, **kwargs)
            if not search_params and '_search_params' in self.__dict__:
                del self._search_params
            else:
                self._search_params = search_params

    def _collect_doc_classes(self):
        return set().union(
            *map(
                collect_doc_classes,
                [
                    self._q,
                    self._source,
                    self._filters,
                    self._post_filters,
                    tuple(self._aggregations.values()),
                    self._order_by,
                    self._rescores,
                ]
            )
        )

    def _get_doc_cls(self):
        if self._doc_cls:
            doc_cls = self._doc_cls
        else:
            doc_cls = self._collect_doc_classes()

        if not doc_cls:
            warnings.warn('Cannot determine document class')
            return None

        return doc_cls

    def _get_doc_type(self, doc_cls=None):
        doc_cls = doc_cls or self._get_doc_cls()
        if isinstance(doc_cls, collections.Iterable):
            return ','.join(d.__doc_type__ for d in doc_cls)
        elif self._doc_type:
            return self._doc_type
        elif doc_cls:
            return doc_cls.__doc_type__

    def get_context(self, compiler=None):
        return SearchQueryContext(self, compiler or self._compiler)

    @cached_property
    def result(self):
        doc_cls = self._get_doc_cls()
        doc_type = self._get_doc_type(doc_cls)
        return (self._index or self._cluster).search(
            self,
            doc_type=doc_type,
            **(self._search_params or {})
        )

    @property
    def results(self):
        return self.result

    def count(self):
        res = self._index.count(
            self.get_context().get_filtered_query(wrap_function_score=False),
            doc_type=self._get_doc_type(),
            routing=self._search_params.get('routing'),
        )
        return res.count

    def exists(self, refresh=None):
        res = self._index.exists(
            self.get_context().get_filtered_query(wrap_function_score=False),
            self._get_doc_type(),
            refresh=refresh,
            routing=self._search_params.get('routing'),
        )
        return res.exists

    def delete(self, timeout=None, consistency=None, replication=None):
        return self._index.delete_by_query(
            self.get_context().get_filtered_query(wrap_function_score=False),
            self._get_doc_type(),
            timeout=timeout,
            consistency=consistency,
            replication=replication,
        )

    def __iter__(self):
        if self._iter_instances:
            return iter(doc.instance for doc in self.result.hits if doc.instance)
        return iter(self.result)

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


class SearchQueryContext(object):
    def __init__(self, search_query, compiler):
        self.compiler = compiler
        
        self.q = search_query._q
        self.source = search_query._source
        self.filters = search_query._filters
        self.filters_meta = search_query._filters_meta
        self.post_filters = search_query._post_filters
        self.post_filters_meta = search_query._post_filters_meta
        self.order_by = search_query._order_by
        self.aggregations = search_query._aggregations
        self.function_score = search_query._function_score
        self.function_score_params = search_query._function_score_params
        self.limit = search_query._limit
        self.offset = search_query._offset
        self.rescores = search_query._rescores
        self.suggest = search_query._suggest
        
        self.cluster = search_query._cluster
        self.index = search_query._index
        self.doc_cls = search_query._doc_cls
        self.doc_type = search_query._doc_type

        self.search_params = search_query._search_params

        self.instance_mapper = search_query._instance_mapper
        self.iter_instances = search_query._iter_instances

    def get_query(self, wrap_function_score=True):
        return self.compiler.get_query(self, wrap_function_score=wrap_function_score)

    def get_filtered_query(self, wrap_function_score=True):
        return self.compiler.get_filtered_query(self, wrap_function_score=wrap_function_score)

    def iter_filters_with_meta(self):
        return zip(self.filters, self.filters_meta)

    def iter_filters(self):
        return iter(self.filters)

    def iter_post_filters_with_meta(self):
        return zip(self.post_filters, self.post_filters_meta)

    def iter_post_filters(self):
        return iter(self.post_filters)
