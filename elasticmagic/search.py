import warnings
import collections

from .compat import zip
from .util import _with_clone
from .util import cached_property, merge_params, collect_doc_classes
from .compiler import DefaultCompiler
from .expression import Params, Source, Highlight, Rescore


__all__ = ['SearchQuery']


class SearchQuery(object):
    """Elasticsearch search query construction object.

    :class:`.SearchQuery` object is usually instantiated by calling
    :func:`Index.search_query()` method.

    See :func:`Index.search_query()` for more details.

    .. testsetup:: *

       import datetime

       from elasticmagic import SearchQuery, DynamicDocument

       class PostDocument(DynamicDocument):
           __doc_type__ = 'post'
    """

    __visit_name__ = 'search_query'

    _q = None
    _source = None
    _fields = None
    _filters = ()
    _filters_meta = ()
    _post_filters = ()
    _post_filters_meta = ()
    _order_by = ()
    _aggregations = Params()
    _function_score = ()
    _function_score_params = Params()
    _boost_score = ()
    _boost_score_params = Params()
    _limit = None
    _offset = None
    _min_score = None
    _rescores = ()
    _suggest = Params()
    _highlight = Params()
    _script_fields = Params()

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
        self._compiler = _compiler or DefaultCompiler().compiled_query

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

    def to_dict(self, compiler=None):
        return (compiler or self._compiler)(self).params

    @_with_clone
    def source(self, *args, **kwargs):
        """Controls which fields of the document ``_source`` field to retrieve.

        .. _fields_arg:

        :param \*fields: list of fields which should be returned by \
        elasticsearch. Can be one of the following types:

           - field expression, for example: ``PostDocument.title``
           - ``str`` means field name or glob pattern. For example: ``"title"``,
             ``"user.*"``
           - ``False`` disables retrieving source
           - ``True`` enables retrieving all source document
           - ``None`` cancels source filtering applied before

        :param include: list of fields to include

        :param exclude: list of fields to exclude

        Example:

        .. testcode:: source

           search_query = SearchQuery().source(PostDocument.title, 'user.*')

        .. testcode:: source

           assert search_query.to_dict() == {'_source': ['title', 'user.*']}

        See `source filtering <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-source-filtering.html>`_
        for more information.
        """
        if len(args) == 1 and args[0] is None:
            if '_source' in self.__dict__:
                del self._source
        elif len(args) == 1 and isinstance(args[0], bool):
            self._source = Source(args[0], **kwargs)
        else:
            self._source = Source(args, **kwargs)

    @_with_clone
    def fields(self, *args):
        if len(args) == 1 and args[0] is None:
            if '_fields' in self.__dict__:
                del self._fields
        elif len(args) == 1 and isinstance(args[0], bool):
            self._fields = args[0]
        else:
            self._fields = args

    @_with_clone
    def query(self, q):
        """Replaces query clause. Elasticsearch's query clause will calculate
        ``_score`` for every matching document.

        :param q: query expression. Existing query can be cancelled by passing \
        ``None``.

        .. testcode:: query

           search_query = SearchQuery().query(
               PostDocument.title.match('test', minimum_should_match='100%'))

        .. testcode:: query

           assert search_query.to_dict() == {
               'query': {'match': {'title': {
                   'query': 'test',
                   'minimum_should_match': '100%'}}}}
        """
        if q is None:
            if '_q' in self.__dict__:
                del self._q
        else:
            self._q = q

    @_with_clone
    def filter(self, *filters, **kwargs):
        """Adds a filters into elasticsearch
        `filter context <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html>`_.

        Multiple expressions may be specified, so they will be joined together
        using ``Bool.must`` expression.

        Returns new :class:`.SearchQuery` object with applied filters.

        .. testcode:: filter

           search_query = SearchQuery().filter(
               PostDocument.status == 'published',
               PostDocument.publish_date >= datetime.date(2015, 1, 1),
           )

        .. testcode:: filter

           assert search_query.to_dict() == {
               'query': {'filtered': {'filter': {'bool': {'must': [
                   {'term': {'status': 'published'}},
                   {'range': {'publish_date': {'gte': datetime.date(2015, 1, 1)}}}]}}}}}

        Filter expression can be a python dictionary object:

        .. testcode:: filter

           search_query = SearchQuery().filter({'term': {'status': 'published'}})

        """
        meta = kwargs.pop('meta', None)
        self._filters = self._filters + filters
        self._filters_meta = self._filters_meta + (meta,) * len(filters)

    @_with_clone
    def post_filter(self, *filters, **kwargs):
        """Adds a filters into elasticsearch
        `post filter context <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-post-filter.html>`_.

        All parameters have the same meaning as for
        :meth:`.filter` method.
        """
        if len(filters) == 1 and filters[0] is None:
            if '_post_filters' in self.__dict__:
                del self._post_filters
        else:
            meta = kwargs.pop('meta', None)
            self._post_filters = self._post_filters + filters
            self._post_filters_meta = \
                self._post_filters_meta + (meta,) * len(filters)

    @_with_clone
    def order_by(self, *orders):
        """Apply sorting criterion to the search query. Corresponds elasticsearch's
        `sort <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html>`_.

        .. testcode:: order_by

           search_query = SearchQuery().order_by(
               PostDocument.publish_date.desc(),
               PostDocument._score,
           )
           assert search_query.to_dict() == {
               'sort': [
                   {'publish_date': 'desc'},
                   '_score'
               ]
           }

        When called with single ``None`` argument clears any sorting criterion
        applied before:

        .. testcode:: order_by

           search_query = SearchQuery().order_by(None)
           assert search_query.to_dict() == {}
        """
        if len(orders) == 1 and orders[0] is None:
            if '_order_by' in self.__dict__:
                del self._order_by
        else:
            self._order_by = self._order_by + orders

    @_with_clone
    def aggregations(self, *args, **kwargs):
        """Adds `aggregations <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html>`_
        to the search query.

        :param \*aggs: dictionaries with aggregations. Can be ``None`` that \
        cleans up previous aggregations.

        After executing the query you can get aggregation result by its name
        calling :meth:`SearchResult.get_aggregation` method.

        .. testcode:: aggs

           from elasticmagic import agg

           search_query = SearchQuery().aggregations({
               'stars': agg.Terms(PostDocument.stars, size=50, aggs={
                   'profit': agg.Sum(PostDocument.profit)})})

        .. testcode:: aggs

           assert search_query.to_dict() == {
               'aggregations': {
                   'stars': {'terms': {'field': 'stars', 'size': 50},
                       'aggregations': {
                           'profit': {'sum': {'field': 'profit'}}}}}}
        """
        if len(args) == 1 and args[0] is None:
            if '_aggregations' in self.__dict__:
                del self._aggregations
        else:
            self._aggregations = merge_params(self._aggregations, args, kwargs)

    aggs = aggregations

    @_with_clone
    def function_score(self, *args, **kwargs):
        """Adds `function scores <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html>`_
        to the search query.

        :param \*functions: list of function scores.

        .. testcode:: function_score

           from elasticmagic import Weight, FieldValueFactor

           search_query = SearchQuery(PostDocument.title.match('test')).function_score(
               Weight(2, filter=PostDocument.created_date == 'now/d'),
               FieldValueFactor(PostDocument.popularity, factor=1.2, modifier='sqrt'))

        .. testcode:: function_score

           assert search_query.to_dict() == {
               'query': {
                   'function_score': {
                       'query': {'match': {'title': 'test'}},
                       'functions': [
                           {'weight': 2,
                            'filter': {'term': {'created_date': 'now/d'}}},
                           {'field_value_factor': {'field': 'popularity', 'factor': 1.2, 'modifier': 'sqrt'}}]}}}
        """

        if args == (None,):
            if '_function_score' in self.__dict__:
                del self._function_score
                del self._function_score_params
        else:
            self._function_score = self._function_score + args
            self._function_score_params = Params(
                dict(self._function_score_params), **kwargs)

    @_with_clone
    def boost_score(self, *args, **kwargs):
        """Adds one more level of the function_score query with default
        sum modes. It is especially useful for complex ordering scenarios.

        :param \*functions: See :meth:`.function_score`

        .. testcode:: boost_score

           from elasticmagic import Factor, ScriptScore, Script

           search_query = (
               SearchQuery(PostDocument.title.match('test'))
               .function_score(
                   # Slightly boost hits on post popularity
                   Factor(PostDocument.popularity, modifier='sqrt'))
               .boost_score(
                   # Display advertized posts higher than any others
                   ScriptScore(Script(inline='log10(10.0 + doc[cpc_field].value)',
                                      params={'cpc_field': PostDocument.adv_cpc}),
                               weight=1000,
                               filter=PostDocument.adv_cpc > 0))
           )

        .. testcode:: boost_score

           assert search_query.to_dict() == {
               'query': {
                   'function_score': {
                       'query': {
                           'function_score': {
                               'query': {'match': {'title': 'test'}},
                               'functions': [
                                   {'field_value_factor': {'field': 'popularity',
                                                           'modifier': 'sqrt'}}]}},
                       'functions': [
                           {'script_score': {'script': {
                                'inline': 'log10(10.0 + doc[cpc_field].value)',
                                'params': {'cpc_field': 'adv_cpc'}}},
                            'filter': {'range': {'adv_cpc': {'gt': 0}}},
                            'weight': 1000}],
                       'boost_mode': 'sum',
                       'score_mode': 'sum'}}}
        """
        if args == (None,):
            if '_boost_score' in self.__dict__:
                del self._boost_score
                del self._boost_score_params
        else:
            self._boost_score = self._boost_score + args
            self._boost_score_params = Params(
                dict(self._boost_score_params), **kwargs)

    @_with_clone
    def script_fields(self, **kwargs):
        self._script_fields = Params(kwargs)

    @_with_clone
    def limit(self, limit):
        """Sets size of the maximum amount of hits. Used for pagination.

        See `from / size <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html>`_
        """
        self._limit = limit

    size = limit

    @_with_clone
    def offset(self, offset):
        """Sets the offset - the number of hits to skip. Used for pagination.

        See `from / size <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html>`_
        """
        self._offset = offset

    from_ = offset

    @_with_clone
    def min_score(self, min_score):
        self._min_score = min_score

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
    def highlight(
            self, fields=None, type=None, pre_tags=None, post_tags=None,
            fragment_size=None, number_of_fragments=None, order=None,
            encoder=None, require_field_match=None, boundary_max_scan=None,
            highlight_query=None, matched_fields=None, fragment_offset=None,
            no_match_size=None, phrase_limit=None,
            **kwargs
    ):
        """Highlights search results.

        .. testcode:: highlight

           from elasticmagic import MultiMatch

           search_query = (
               SearchQuery(MultiMatch('The quick brown fox',
                                      [PostDocument.title, PostDocument.content]))
               .highlight([PostDocument.title, PostDocument.content])
           )

        When processing search result you can get hit highlight by calling
        :meth:`.Document.get_highlight`.
        """
        self._highlight = Highlight(
            fields=fields, type=type, pre_tags=pre_tags, post_tags=post_tags,
            fragment_size=fragment_size,
            number_of_fragments=number_of_fragments,
            order=order, encoder=encoder,
            require_field_match=require_field_match,
            boundary_max_scan=boundary_max_scan,
            highlight_query=highlight_query, matched_fields=matched_fields,
            fragment_offset=fragment_offset, no_match_size=no_match_size,
            phrase_limit=phrase_limit,
            **kwargs
        )

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
                    self._fields,
                    self._filters,
                    self._post_filters,
                    tuple(self._aggregations.values()),
                    self._order_by,
                    self._rescores,
                    self._highlight,
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
            return ','.join(set(d.__doc_type__ for d in doc_cls))
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
        """Executes current query and returns processed :class:`SearchResult`
        object. Caches result so subsequence calls with the same search query
        will return cached value.
        """
        return self.result

    def count(self):
        """Executes current query and returns number of documents matched the
        query. Uses `count api <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html>`_.
        """
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
            return iter(doc.instance for doc in self.result.hits
                        if doc.instance)
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
        self.fields = search_query._fields
        self.filters = search_query._filters
        self.filters_meta = search_query._filters_meta
        self.post_filters = search_query._post_filters
        self.post_filters_meta = search_query._post_filters_meta
        self.order_by = search_query._order_by
        self.aggregations = search_query._aggregations
        self.function_score = search_query._function_score
        self.function_score_params = search_query._function_score_params
        self.boost_score = search_query._boost_score
        self.boost_score_params = search_query._boost_score_params
        self.limit = search_query._limit
        self.offset = search_query._offset
        self.min_score = search_query._min_score
        self.rescores = search_query._rescores
        self.suggest = search_query._suggest
        self.highlight = search_query._highlight

        self.cluster = search_query._cluster
        self.index = search_query._index
        self.doc_cls = search_query._doc_cls
        self.doc_type = search_query._doc_type
        self.script_fields = search_query._script_fields

        self.search_params = search_query._search_params

        self.instance_mapper = search_query._instance_mapper
        self.iter_instances = search_query._iter_instances

    def get_query(self, wrap_function_score=True):
        return self.compiler.get_query(
            self, wrap_function_score=wrap_function_score)

    def get_filtered_query(self, wrap_function_score=True):
        return self.compiler.get_filtered_query(
            self, wrap_function_score=wrap_function_score)

    def iter_filters_with_meta(self):
        return zip(self.filters, self.filters_meta)

    def iter_filters(self):
        return iter(self.filters)

    def iter_post_filters_with_meta(self):
        return zip(self.post_filters, self.post_filters_meta)

    def iter_post_filters(self):
        return iter(self.post_filters)
