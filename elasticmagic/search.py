"""
.. testsetup:: *

   from elasticmagic.compiler import Compiler_5_0
"""
import warnings
from abc import ABCMeta
from collections import namedtuple, OrderedDict

from .compat import zip, with_metaclass, string_types
from .compat import Iterable
from .util import _with_clone
from .util import merge_params, collect_doc_classes
from .expression import Params, Source, Highlight, Rescore, Script

__all__ = [
    'BaseSearchQuery', 'SearchQuery', 'SearchQueryContext',
    'FunctionScoreSettings', 'GENERAL_FUNCTION_SCORE', 'BOOST_FUNCTION_SCORE'
]


_FunctionScore = namedtuple('_FunctionScore', ['functions', 'params'])


class FunctionScoreSettings(object):
    def __init__(
            self, name, score_mode=None, boost_mode=None, boost=None,
            max_boost=None, min_score=None, **kwargs
    ):
        self.name = name
        self.settings = Params(
            score_mode=score_mode, boost_mode=boost_mode, boost=boost,
            max_boost=max_boost, min_score=min_score, **kwargs
        )


GENERAL_FUNCTION_SCORE = FunctionScoreSettings(
    'GENERAL',
    # TODO: Should we specify default settings explicitly?
    # score_mode='multiply', boost_mode='multiply'
)

BOOST_FUNCTION_SCORE = FunctionScoreSettings(
    'BOOST', score_mode='sum', boost_mode='sum'
)


class BaseSearchQuery(with_metaclass(ABCMeta)):
    _q = None
    _source = None
    _fields = None
    _filters = ()
    _filters_meta = ()
    _post_filters = ()
    _post_filters_meta = ()
    _order_by = ()
    _aggregations = Params()
    _ext = Params()
    _function_scores = OrderedDict([
        (
            BOOST_FUNCTION_SCORE.name,
            _FunctionScore((), BOOST_FUNCTION_SCORE.settings)
        ),
        (
            GENERAL_FUNCTION_SCORE.name,
            _FunctionScore((), GENERAL_FUNCTION_SCORE.settings)
        ),
    ])
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

    _cached_result = None

    def __init__(
            self, q=None,
            cluster=None, index=None, doc_cls=None, doc_type=None,
            routing=None, preference=None, timeout=None, search_type=None,
            query_cache=None, terminate_after=None, scroll=None, stats=None,
            **kwargs
    ):
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
            stats=stats,
            **kwargs
        )
        if search_params:
            self._search_params = search_params

    def clone(self):
        """Clones this query so you can modify both queries independently.
        """
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = {
            k: v for k, v in self.__dict__.items()
            if not k.startswith('_cached_')
        }
        return q

    @_with_clone
    def source(self, *fields, **kwargs):
        """Controls which fields of the document's ``_source`` field
        to retrieve.

        .. _fields_arg:

        :param \\*fields: list of fields which should be returned by
           elasticsearch. Can be one of the following types:

           - field expression, for example: ``PostDocument.title``
           - ``str`` means field name or glob pattern. For example:
             ``"title"``, ``"user.*"``
           - ``False`` disables retrieving source
           - ``True`` enables retrieving all source document
           - ``None`` cancels source filtering applied before

        :param include: list of fields to include

        :param exclude: list of fields to exclude

        Example:

        .. testcode:: source

           search_query = SearchQuery().source(PostDocument.title, 'user.*')

        .. testcode:: source

           assert search_query.to_dict(Compiler_5_0) == {'_source': ['title', 'user.*']}

        See `source filtering <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-source-filtering.html>`_
        for more information.
        """  # noqa:E501
        if len(fields) == 1 and fields[0] is None:
            if '_source' in self.__dict__:
                del self._source
        elif len(fields) == 1 and isinstance(fields[0], bool):
            self._source = Source(fields[0], **kwargs)
        else:
            self._source = Source(fields, **kwargs)

    @_with_clone
    def stored_fields(self, *fields):
        """Allows to load fields that marked as ``store: true``.

        Example:

        .. testcode:: stored_fields

           search_query = SearchQuery().stored_fields(PostDocument.rank)

        .. testcode:: stored_fields

           assert search_query.to_dict(Compiler_5_0) == {'stored_fields': ['rank']}

        See `stored fields <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-store.html>`_ and
        `stored fields filtering <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-stored-fields.html>`_
        for more information.
        """  # noqa:E501
        if len(fields) == 1 and fields[0] is None:
            if '_fields' in self.__dict__:
                del self._fields
        elif len(fields) == 1 and isinstance(fields[0], bool):
            self._fields = fields[0]
        else:
            self._fields = fields

    def fields(self, *fields):
        return self.stored_fields(*fields)

    @_with_clone
    def script_fields(self, *args, **kwargs):
        """Allows to evaluate fields based on scripts.

        .. testcode:: script_fields

           from elasticmagic import Script

           search_query = SearchQuery().script_fields(
               rating=Script(
                   inline='doc[params.positive_opinions_field].value / '
                          'doc[params.total_opinions_field].value * 5',
                   params={
                       'total_opinions_field': PostDocument.total_opinions,
                       'positive_opinions_field': PostDocument.positive_opinions,
                   }
               )
           )

        .. testcode:: script_fields

           expected = {
               'script_fields': {
                   'rating': {
                       'script': {
                           'inline': 'doc[params.positive_opinions_field].value / '
                                     'doc[params.total_opinions_field].value * 5',
                           'params': {
                               'total_opinions_field': 'total_opinions',
                               'positive_opinions_field': 'positive_opinions'
                           }
                       }
                   }
               }
           }

           assert search_query.to_dict(Compiler_5_0) == {
               'script_fields': {
                   'rating': {
                       'script': {
                           'inline': 'doc[params.positive_opinions_field].value / '
                                     'doc[params.total_opinions_field].value * 5',
                           'params': {
                               'total_opinions_field': 'total_opinions',
                               'positive_opinions_field': 'positive_opinions'
                           }
                       }
                   }
               }
           }

        See `script fields <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-script-fields.html>`_
        """  # noqa:E501
        if len(args) == 1 and args[0] is None:
            if '_script_fields' in self.__dict__:
                del self._script_fields
        for key in kwargs:
            if isinstance(kwargs[key], Script):
                kwargs[key] = dict(script=kwargs[key])
        script_fields = dict()
        for key in self._script_fields:
            script_fields[key] = self._script_fields[key]
        script_fields.update(kwargs)
        self._script_fields = Params(script_fields)

    @_with_clone
    def query(self, q):
        """Replaces query clause. Elasticsearch's query clause will calculate
        ``_score`` for every matching document.

        :param q: query expression. Existing query can be cancelled by passing
           ``None``.

        .. testcode:: query

           search_query = SearchQuery().query(
               PostDocument.title.match('test', minimum_should_match='100%'))

        .. testcode:: query

           assert search_query.to_dict(Compiler_5_0) == {
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

        Returns new :class:`.SearchQuery` object with applied filters.

        .. testcode:: filter

           search_query = SearchQuery().filter(
               PostDocument.status == 'published',
               PostDocument.publish_date >= datetime.date(2015, 1, 1),
           )

        .. testcode:: filter

           assert search_query.to_dict(Compiler_5_0) == {
               'query': {'bool': {'filter': [
                   {'term': {'status': 'published'}},
                   {'range': {
                       'publish_date': {
                           'gte': datetime.date(2015, 1, 1)}}}]}}}

        Filter expression can be a python dictionary object:

        .. testcode:: filter

           search_query = SearchQuery().filter({'term': {'status': 'published'}})

        """  # noqa:E501
        meta = kwargs.pop('meta', None)
        self._filters = self._filters + filters
        self._filters_meta = self._filters_meta + (meta,) * len(filters)

    @_with_clone
    def post_filter(self, *filters, **kwargs):
        """Adds a filters into elasticsearch
        `post filter context <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-post-filter.html>`_.

        All parameters have the same meaning as for
        :meth:`.filter` method.
        """  # noqa:E501
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
        """Apply sorting criterion to the search query.
        Corresponds elasticsearch's
        `sort <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html>`_.

        .. testcode:: order_by

           search_query = SearchQuery().order_by(
               PostDocument.publish_date.desc(),
               PostDocument._score,
           )
           assert search_query.to_dict(Compiler_5_0) == {
               'sort': [
                   {'publish_date': 'desc'},
                   '_score'
               ]
           }

        When called with single ``None`` argument clears any sorting criterion
        applied before:

        .. testcode:: order_by

           search_query = SearchQuery().order_by(None)
           assert search_query.to_dict(Compiler_5_0) == {}
        """  # noqa:E501
        if len(orders) == 1 and orders[0] is None:
            if '_order_by' in self.__dict__:
                del self._order_by
        else:
            self._order_by = self._order_by + orders

    @_with_clone
    def aggregations(self, *args, **kwargs):
        """Adds `aggregations <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html>`_
        to the search query.

        :param \\*aggs: dictionaries with aggregations. Can be ``None`` that
           cleans up previous aggregations.

        After executing the query you can get aggregation result by its name
        calling :meth:`SearchResult.get_aggregation` method.

        .. testcode:: aggs

           from elasticmagic import agg

           search_query = SearchQuery().aggregations({
               'stars': agg.Terms(PostDocument.stars, size=50, aggs={
                   'profit': agg.Sum(PostDocument.profit)})})

        .. testcode:: aggs

           assert search_query.to_dict(Compiler_5_0) == {
               'aggregations': {
                   'stars': {'terms': {'field': 'stars', 'size': 50},
                       'aggregations': {
                           'profit': {'sum': {'field': 'profit'}}}}}}
        """  # noqa:E501
        if len(args) == 1 and args[0] is None:
            if '_aggregations' in self.__dict__:
                del self._aggregations
        else:
            self._aggregations = merge_params(self._aggregations, args, kwargs)

    def aggs(self, *args, **kwargs):
        """A shortcut for the :meth:`.aggregations` method
        """
        return self.aggregations(*args, **kwargs)

    @_with_clone
    def ext(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            if '_ext' in self.__dict__:
                del self._ext
        else:
            self._ext = merge_params(self._ext, args, kwargs)

    @_with_clone
    def function_score_settings(self, *function_score_settings):
        """Adds or updates function score level.
        Levels will be inserted before existing.
        """
        function_scores = OrderedDict()
        for fs_settings in function_score_settings:
            if fs_settings.name in self._function_scores:
                fs = self._function_score[fs_settings.name]
                self._function_score[fs_settings.name] = _FunctionScore(
                    fs.functions, fs_settings.settings
                )
            else:
                function_scores[fs_settings.name] = _FunctionScore(
                    (), fs_settings.settings
                )
        function_scores.update(self._function_scores)
        self._function_scores = function_scores

    @_with_clone
    def function_score(self, *args, **kwargs):
        """Adds `function scores <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html>`_
        to the search query.

        :param \\*functions: list of function scores.

        .. testcode:: function_score

           from elasticmagic import Weight, FieldValueFactor

           search_query = (
               SearchQuery(PostDocument.title.match('test'))
               .function_score(
                   Weight(2, filter=PostDocument.created_date == 'now/d'),
                   FieldValueFactor(
                       PostDocument.popularity, factor=1.2, modifier='sqrt'
                   )
               )
           )

        .. testcode:: function_score

           assert search_query.to_dict(Compiler_5_0) == {
               'query': {
                   'function_score': {
                       'query': {'match': {'title': 'test'}},
                       'functions': [
                           {'weight': 2,
                            'filter': {'term': {'created_date': 'now/d'}}},
                           {'field_value_factor': {
                                'field': 'popularity',
                                'factor': 1.2,
                                 'modifier': 'sqrt'}}]}}}
        """  # noqa:E501
        if len(args) >= 1 and isinstance(args[0], FunctionScoreSettings):
            self._function_score(args[0].name, args[1:], kwargs)
        else:
            self._function_score(GENERAL_FUNCTION_SCORE.name, args, kwargs)

    @_with_clone
    def boost_score(self, *args, **kwargs):
        """Adds one more level of the function_score query with default
        sum modes. It is especially useful for complex ordering scenarios.

        :param \\*functions: See :meth:`.function_score`

        .. testcode:: boost_score

           from elasticmagic import Factor, ScriptScore, Script

           search_query = (
               SearchQuery(PostDocument.title.match('test'))
               .function_score(
                   # Slightly boost hits on post popularity
                   Factor(PostDocument.popularity, modifier='sqrt'))
               .boost_score(
                   # Display advertized posts higher than any others
                   ScriptScore(
                       Script(inline='log10(10.0 + doc[cpc_field].value)',
                              params={'cpc_field': PostDocument.adv_cpc}),
                       weight=1000,
                       filter=PostDocument.adv_cpc > 0))
           )

        .. testcode:: boost_score

           assert search_query.to_dict(Compiler_5_0) == {
               'query': {
                   'function_score': {
                       'query': {
                           'function_score': {
                               'query': {'match': {'title': 'test'}},
                               'functions': [
                                   {'field_value_factor': {
                                       'field': 'popularity',
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
        self._function_score(BOOST_FUNCTION_SCORE.name, args, kwargs)

    def _function_score(self, key, functions, kwargs):
        if key not in self._function_scores:
            raise ValueError(
                'Cannot find function score: [{}]'.format(key)
            )
        self._function_scores = OrderedDict(self._function_scores)
        if functions == (None,):
            fs = self._function_scores[key]
            self._function_scores[key] = _FunctionScore((), fs.params)
        else:
            self._function_scores[key] = _FunctionScore(
                self._function_scores[key].functions + functions,
                Params(dict(self._function_scores[key].params), **kwargs)
            )

    @_with_clone
    def limit(self, limit):
        """Sets size of the maximum amount of hits. Used for pagination.

        See `from / size <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html>`_
        """  # noqa:E501
        self._limit = limit

    size = limit

    @_with_clone
    def offset(self, offset):
        """Sets the offset - the number of hits to skip. Used for pagination.

        See `from / size <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html>`_
        """  # noqa:E501
        self._offset = offset

    from_ = offset

    @_with_clone
    def min_score(self, min_score):
        """Excludes hits with a ``_score`` less then ``min_score``. See
        `min score <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-min-score.html>`_
        """  # noqa:E501
        self._min_score = min_score

    @_with_clone
    def rescore(self, rescorer, window_size=None):
        """Adds a rescorer for the query. See
        `rescoring <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-rescore.html>`_
        """  # noqa:E501
        if rescorer is None:
            if '_rescores' in self.__dict__:
                del self._rescores
            return
        rescore = Rescore(rescorer, window_size=window_size)
        self._rescores = self._rescores + (rescore,)

    @_with_clone
    def suggest(self, *args, **kwargs):
        """Adds `suggesters <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters.html>`_
        to the query"""  # noqa:E501
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
               SearchQuery(
                   MultiMatch('The quick brown fox',
                              [PostDocument.title, PostDocument.content])
               )
               .highlight([PostDocument.title, PostDocument.content])
           )

        When processing search result you can get hit highlight by calling
        :meth:`.Document.get_highlight`.

        See `highlighting <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-highlighting.html>`_
        for details.
        """  # noqa:E501
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

    def with_stats(self, stats):
        return self.with_search_params(stats=stats)

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
                    tuple(
                        fs.functions for fs in self._function_scores.values()
                    ),
                    tuple(self._aggregations.values()),
                    self._order_by,
                    self._rescores,
                    self._highlight,
                    self._ext,
                ]
            )
        )

    @property
    def _index_or_cluster(self):
        if not self._index and not self._cluster:
            raise ValueError('Search query is not bound to index or cluster')
        return self._index or self._cluster

    def get_compiler_context(self):
        return SearchQueryContext(self)

    get_context = get_compiler_context

    def slice(self, offset, limit):
        """Applies offset and limit to the query."""
        sliced_query, _ = self._prepare_slice(slice(offset, limit))
        return sliced_query

    def _prepare_slice(self, k):
        if not isinstance(k, (slice, int)):
            raise TypeError('Index must be of type int or slice')

        if isinstance(k, slice):
            start, stop, step = k.start, k.stop, k.step or 1
            if (
                    start is not None and start < 0 or
                    stop is not None and stop < 0
            ):
                raise ValueError(
                    'Negative slices are not supported'
                )
            if step is not None and step != 1:
                raise ValueError(
                    'Slices with step different from 1 are not supported'
                )

            clone = self.clone()
            if start is not None:
                clone._offset = start
            if stop is not None:
                if start is None:
                    clone._limit = stop
                else:
                    clone._limit = stop - start
            return clone, True
        else:
            if k < 0:
                raise ValueError('Negative indexes are not supported')
            clone = self.clone()
            clone._offset = k
            clone._limit = 1
            return clone, False

    def _iter_result(self, res):
        if self._iter_instances:
            return iter(
                doc.instance for doc in res.hits
                if doc.instance
            )
        return iter(res)


class SearchQuery(BaseSearchQuery):
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

    def get_compiler(self):
        return self._index_or_cluster.get_compiler()

    def get_query_compiler(self):
        return self.get_compiler().compiled_query

    def to_dict(self, compiler=None):
        """Compiles the query and returns python dictionary that can be
        serialized to json.
        """
        compiler = compiler or self.get_compiler()
        return compiler.compiled_query(self).body

    def get_result(self):
        """Executes current query and returns processed :class:`SearchResult`
        object. Caches result so subsequent calls with the same search query
        will return cached value.
        """
        if self._cached_result is not None:
            return self._cached_result

        self._cached_result = self._index_or_cluster.search(self)
        return self._cached_result

    @property
    def result(self):
        warnings.warn(
            'Field `result` is deprecated, use `get_result` method instead',
            DeprecationWarning
        )
        return self.get_result()

    @property
    def results(self):
        warnings.warn(
            'Field `results` is deprecated, use `get_result` method instead',
            DeprecationWarning
        )
        return self.get_result()

    def count(self):
        """Executes current query and returns number of documents matched the
        query. Uses `count api <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html>`_.
        """  # noqa:E501
        return self._index_or_cluster.count(self).count

    def exists(self):
        """Executes current query optimized for checking that
        there are at least 1 matching document. This method is an analogue of
        the old `exists search api <https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-exists.html>`_
        For Elasticsearch 5.x and more it emulates the exists API using
        `size=0` and `terminate_after=1`.
        """  # noqa:E501
        return self._index_or_cluster.exists(self).exists

    def explain(self, doc_or_id, doc_cls=None, routing=None, **kwargs):
        """Computes a score explanation for the current search query and the document.

        :return: :class:`.result.ExplainResult`

        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-explain.html
        """  # noqa:E501
        return self._index_or_cluster.explain(
            self, doc_or_id, doc_cls=doc_cls, routing=routing, **kwargs
        )

    def delete(
            self, conflicts=None, refresh=None, timeout=None,
            scroll=None, scroll_size=None,
            wait_for_completion=None, requests_per_second=None,
            **kwargs
    ):
        """Deletes all documents that match the query.

        .. note::
           As it uses `delete by query api <https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html>`_
           the documents that were changed between the time when a snapshot was
           taken and when the delete request was processed won't be deleted.
        """  # noqa:E501
        return self._index_or_cluster.delete_by_query(
            self,
            conflicts=conflicts,
            refresh=refresh,
            timeout=timeout,
            scroll=scroll,
            scroll_size=scroll_size,
            wait_for_completion=wait_for_completion,
            requests_per_second=requests_per_second,
            **kwargs
        )

    def __iter__(self):
        return self._iter_result(self.get_result())

    def __getitem__(self, k):
        clone, is_slice = self._prepare_slice(k)
        if is_slice:
            return list(clone)
        else:
            return list(clone)[0]


class SearchQueryContext(object):
    __visit_name__ = 'search_query_context'

    def __init__(self, search_query):
        self.q = search_query._q
        self.source = search_query._source
        self.fields = search_query._fields
        self.filters = search_query._filters
        self.filters_meta = search_query._filters_meta
        self.post_filters = search_query._post_filters
        self.post_filters_meta = search_query._post_filters_meta
        self.order_by = search_query._order_by
        self.aggregations = search_query._aggregations
        self.ext = search_query._ext
        self.function_scores = search_query._function_scores
        self.limit = search_query._limit
        self.offset = search_query._offset
        self.min_score = search_query._min_score
        self.rescores = search_query._rescores
        self.suggest = search_query._suggest
        self.highlight = search_query._highlight

        self.cluster = search_query._cluster
        self.index = search_query._index
        doc_cls = search_query._doc_cls
        if not doc_cls:
            doc_classes = search_query._collect_doc_classes()
        elif not isinstance(doc_cls, Iterable):
            doc_classes = [doc_cls]
        else:
            doc_classes = doc_cls
        self.doc_classes = tuple(doc_classes)

        if not search_query._doc_type:
            doc_types = []
        elif isinstance(search_query._doc_type, string_types):
            doc_types = [
                t.strip() for t in search_query._doc_type.split(',')
            ]
        else:
            doc_types = list(search_query._doc_type)
        self.doc_types = self._get_unique_doc_types(
            doc_types, self.doc_classes
        )

        self.script_fields = search_query._script_fields

        self.search_params = search_query._search_params

        self.instance_mapper = search_query._instance_mapper
        self.iter_instances = search_query._iter_instances

    @staticmethod
    def _get_unique_doc_types(doc_types=None, doc_classes=None):
        doc_types = list(doc_types) if doc_types else []
        unique_doc_types = set(doc_types)
        if isinstance(doc_classes, (list, tuple, set)):
            for doc_cls in doc_classes:
                doc_type = doc_cls.get_doc_type()
                if doc_type and doc_type not in unique_doc_types:
                    doc_types.append(doc_type)
        return tuple(doc_types)

    def iter_filters_with_meta(self):
        return zip(self.filters, self.filters_meta)

    def iter_filters(self):
        return iter(self.filters)

    def iter_post_filters_with_meta(self):
        return zip(self.post_filters, self.post_filters_meta)

    def iter_post_filters(self):
        return iter(self.post_filters)
