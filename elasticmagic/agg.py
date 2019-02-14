"""
.. testsetup:: min,sum,value-count,top-hits,stats,percentiles,percentile-ranks

   from __future__ import print_function
   from mock import Mock

   from elasticmagic import agg, Cluster, SearchQuery, DynamicDocument
   from elasticmagic.compiler import DefaultCompiler

   class SaleDocument(DynamicDocument):
       __doc_type__ = 'sale'

   class GradeDocument(DynamicDocument):
       __doc_type__ = 'sale'

   class PageLoadDoc(DynamicDocument):
       __doc_type__ = 'load_page'

   def sq(aggs_raw_result):
       cluster = Cluster(Mock(
           search=Mock(
               return_value={
                   'hits': {'max_score': 1, 'total': 1, 'hits': []},
                   'aggregations': aggs_raw_result})),
           compiler=DefaultCompiler)
       return cluster.search_query()
"""
from itertools import chain

from .document import DynamicDocument
from .expression import ParamsExpression, Params
from .compat import force_unicode
from .types import instantiate, Type
from .util import _with_clone, cached_property, maybe_float, merge_params


class AggExpression(ParamsExpression):
    __visit_name__ = 'agg'

    result_cls = None

    def clone(self):
        return self.__class__(**self.params)

    def build_agg_result(
            self, raw_data, doc_cls_map=None, mapper_registry=None,
    ):
        raise NotImplementedError()


class AggResult(object):
    def __init__(self, agg_expr):
        self.expr = agg_expr


class MetricsAgg(AggExpression):
    def build_agg_result(
            self, raw_data, doc_cls_map=None, mapper_registry=None,
    ):
        return self.result_cls(self, raw_data)


class BucketAgg(AggExpression):
    __visit_name__ = 'bucket_agg'

    def __init__(self, aggs=None, **kwargs):
        super(BucketAgg, self).__init__(**kwargs)
        self._aggregations = Params(
            aggs or {}, **kwargs.pop('aggregations', {})
        )

    def clone(self):
        return self.__class__(aggs=self._aggregations, **self.params)

    @_with_clone
    def aggregations(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            self._aggregations = Params()
        else:
            self._aggregations = merge_params(self._aggregations, args, kwargs)

    aggs = aggregations

    def build_agg_result(
            self, raw_data, doc_cls_map=None, mapper_registry=None,
    ):
        return self.result_cls(
            self, raw_data,
            doc_cls_map=doc_cls_map,
            mapper_registry=mapper_registry,
        )


class SingleValueMetricsAggResult(AggResult):
    def __init__(self, agg_expr, raw_data):
        super(SingleValueMetricsAggResult, self).__init__(agg_expr)
        # TODO: Do we really need to coerce to float?
        self.value = maybe_float(raw_data['value'])
        self.value_as_string = raw_data.get(
            'value_as_string', force_unicode(raw_data['value'])
        )


class SingleValueMetricsAgg(MetricsAgg):
    result_cls = SingleValueMetricsAggResult

    def __init__(self, field=None, script=None, **kwargs):
        super(SingleValueMetricsAgg, self).__init__(
            field=field, script=script, **kwargs
        )


class MultiValueMetricsAggResult(AggResult):
    def __init__(self, agg_expr, raw_data):
        super(MultiValueMetricsAggResult, self).__init__(agg_expr)
        if 'values' in raw_data:
            self.values = raw_data['values']
        else:
            self.values = raw_data


class MultiValueMetricsAgg(MetricsAgg):
    result_cls = MultiValueMetricsAggResult

    def __init__(self, field=None, script=None, **kwargs):
        super(MultiValueMetricsAgg, self).__init__(
            field=field, script=script, **kwargs
        )


class Min(SingleValueMetricsAgg):
    """A single-value metric aggregation that returns the minimum value among
    all extracted numeric values. See
    `min agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html>`_.

    .. testsetup:: min

       search_query = sq({'min_price': {'value': 10.0}})

    .. testcode:: min

       search_query = search_query.aggs({
           'min_price': agg.Min(SaleDocument.price)
       })
       assert search_query.to_dict() == {
           'aggregations': {
               'min_price': {'min': {'field': 'price'}}}}
       min_price_agg = search_query.get_result().get_aggregation('min_price')
       print(min_price_agg.value)
       print(min_price_agg.value_as_string)

    .. testoutput:: min

       10.0
       10.0
    """  # noqa:E501
    __agg_name__ = 'min'


class Max(SingleValueMetricsAgg):
    """A single-value metric aggregation that returns the maximum value among
    all extracted numeric values. See
    `max agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html>`_.
    """  # noqa:E501
    __agg_name__ = 'max'


class Sum(SingleValueMetricsAgg):
    """A single-value metric aggregation that sums up all extracted numeric
    values. See
    `sum agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html>`_.

    .. testsetup:: sum

       search_query = sq({'prices': {'value': 450.0}})

    .. testcode:: sum

       search_query = search_query.aggs({
           'prices': agg.Sum(SaleDocument.price)
       })
       assert search_query.to_dict() == {
           'aggregations': {
               'prices': {'sum': {'field': 'price'}}}}
       prices_agg = search_query.get_result().get_aggregation('prices')
       print(prices_agg.value)
       print(prices_agg.value_as_string)

    .. testoutput:: sum

       450.0
       450.0
    """  # noqa:E501
    __agg_name__ = 'sum'


class Avg(SingleValueMetricsAgg):
    """A single-value metric aggregation that computes average of all extracted
    numeric values. See
    `avg agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html>`_.
    """  # noqa:E501
    __agg_name__ = 'avg'


class ValueCount(SingleValueMetricsAgg):
    """A single-value metric aggregation that counts the number of all extracted
    values. See
    `value_count agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html>`_.

    .. testsetup:: value-count

       search_query = sq({'types_count': {'value': 7}})

    .. testcode:: value-count

       from elasticmagic import Script

       search_query = search_query.aggs({
           'types_count': agg.ValueCount(script=Script(
               inline='doc[params.field].value',
               params={'field': SaleDocument.type}
           ))
       })
       assert search_query.to_dict() == {
           'aggregations': {
               'types_count': {
                   'value_count': {
                       'script': {
                           'inline': 'doc[params.field].value',
                           'params': {'field': 'type'}}}}}}
       print(search_query.get_result().get_aggregation('types_count').value)

    .. testoutput:: value-count

       7.0
    """  # noqa:E501
    __agg_name__ = 'value_count'


class TopHitsResult(AggResult):
    def __init__(
            self, agg_expr, raw_data, doc_cls_map,
            mapper_registry, instance_mapper,
    ):
        super(TopHitsResult, self).__init__(agg_expr)
        hits_data = raw_data['hits']
        self.total = hits_data['total']
        self.max_score = hits_data['max_score']

        self.hits = []
        for hit in hits_data['hits']:
            doc_cls = doc_cls_map.get(hit['_type'], DynamicDocument)
            self.hits.append(doc_cls(_hit=hit, _result=self))

        if isinstance(instance_mapper, dict):
            self._instance_mappers = instance_mapper
        else:
            self._instance_mappers = {
                doc_cls: instance_mapper for doc_cls in doc_cls_map.values()
            }

        if mapper_registry is None:
            self._mapper_registry = {}
        else:
            self._mapper_registry = mapper_registry

        if self._instance_mappers:
            for instance_mapper in self._instance_mappers.values():
                self._mapper_registry \
                    .setdefault(instance_mapper, []) \
                    .append(self)

    def _populate_instances(self, doc_cls):
        instance_mapper = self._instance_mappers.get(doc_cls)
        hits = list(chain(
            *(
                map(
                    lambda r: filter(
                        lambda hit: isinstance(hit, doc_cls), r.hits
                    ),
                    self._mapper_registry.get(instance_mapper, [self])
                )
            )
        ))
        ids = [hit._id for hit in hits]
        instances = instance_mapper(ids) if instance_mapper else {}
        for hit in hits:
            hit.__dict__['instance'] = instances.get(hit._id)


class TopHits(MetricsAgg):
    """A `top_hits` metric aggregation that groups result set by certain fields
    via a bucket aggregator. See
    `top_hits agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html>`_.

    .. testsetup:: top-hits

       b1 = {
           'key': 'hat',
           'doc_count': 3,
           'top_sales_hits': {'hits': {'total': 3, 'max_score': None, 'hits': [{
               '_index': 'sales', '_type': 'sale', '_id': 'AVnNBmauCQpcRyxw6ChK',
               '_source': {'date': '2015/03/01 00:00:00', 'price': 200}
           }]}}
       }
       b2 = {
           'key': 't-shirt',
           'doc_count': 3,
           'top_sales_hits': {'hits': {'total': 3, 'max_score': None, 'hits': [{
               '_index': 'sales', '_type': 'sale', '_id': 'AVnNBmauCQpcRyxw6ChL',
               '_source': {'date': '2015/03/01 00:00:00', 'price': 175}
           }]}}
       }
       b3 = {
           'key': 'bag',
           'doc_count': 1,
           'top_sales_hits': {'hits': {'total': 1, 'max_score': None, 'hits': [{
               '_index': 'sales', '_type': 'sale', '_id': 'AVnNBmatCQpcRyxw6ChH',
               '_source': {'date': '2015/01/01 00:00:00', 'price': 150}
           }]}}
       }
       search_query = sq({'top_tags': {'buckets': [b1, b2, b3]}})

    .. testcode:: top-hits

       search_query = search_query.aggs({
           'top_tags': agg.Terms(
               SaleDocument.type, size=3,
               aggs={'top_sales_hits': agg.TopHits(
                   size=1,
                   sort=SaleDocument.date.desc(),
                   _source={
                       'includes': [SaleDocument.date, SaleDocument.price]
                   }
               )}
           )
       })
       assert search_query.to_dict() == {
           'aggregations': {
               'top_tags': {
                   'terms': {'field': 'type', 'size': 3},
                   'aggregations': {
                       'top_sales_hits': {
                           'top_hits': {
                               'size': 1,
                               'sort': {'date': 'desc'},
                               '_source': {'includes': ['date', 'price']}}}}}}}
       top_tags = search_query.get_result().get_aggregation('top_tags')
       for tag_bucket in top_tags.buckets:
           top_hit = tag_bucket.get_aggregation('top_sales_hits').hits[0]
           print(
               '{0.key} ({0.doc_count}) - {1.price}'.format(
                   tag_bucket, top_hit
               )
           )

    .. testoutput:: top-hits

       hat (3) - 200
       t-shirt (3) - 175
       bag (1) - 150
    """  # noqa:E501
    __agg_name__ = 'top_hits'

    result_cls = TopHitsResult

    def __init__(
            self, size=None, from_=None, sort=None, _source=None,
            instance_mapper=None, **kwargs
    ):
        super(TopHits, self).__init__(
            size=size, from_=from_, sort=sort, _source=_source, **kwargs
        )
        self._instance_mapper = instance_mapper

    def build_agg_result(
            self, raw_data, doc_cls_map=None, mapper_registry=None
    ):
        doc_cls_map = doc_cls_map or {}
        return self.result_cls(
            self, raw_data, doc_cls_map, mapper_registry, self._instance_mapper
        )


class StatsResult(MultiValueMetricsAggResult):
    def __init__(self, agg_expr, values):
        super(StatsResult, self).__init__(agg_expr, values)
        self.count = self.values['count']
        self.min = self.values['min']
        self.max = self.values['max']
        self.avg = self.values['avg']
        self.sum = self.values['sum']


class Stats(MultiValueMetricsAgg):
    """A multi-value metrics aggregation that computes stats over all extracted
    numeric values. See
    `stats agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html>`_.

    .. testsetup:: stats

       search_query = sq({'grades_stats': {
           'count': 6, 'min': 60, 'max': 98, 'avg': 78.5, 'sum': 471}})

    .. testcode:: stats

       search_query = search_query.aggs({
           'grades_stats': agg.Stats(GradeDocument.grade)
       })
       assert search_query.to_dict() == {
           'aggregations': {
               'grades_stats': {'stats': {'field': 'grade'}}}}
       grades_stats = search_query.get_result().get_aggregation('grades_stats')
       print('count:', grades_stats.count)
       print('min:', grades_stats.min)
       print('max:', grades_stats.max)
       print('avg:', grades_stats.avg)
       print('sum:', grades_stats.sum)

    .. testoutput:: stats

       count: 6
       min: 60
       max: 98
       avg: 78.5
       sum: 471
    """  # noqa:E501
    __agg_name__ = 'stats'

    result_cls = StatsResult

    def __init__(self, field=None, script=None, **kwargs):
        super(Stats, self).__init__(field=field, script=script, **kwargs)


class ExtendedStatsResult(StatsResult):
    def __init__(self, agg_expr, values):
        super(ExtendedStatsResult, self).__init__(agg_expr, values)
        self.sum_of_squares = self.values['sum_of_squares']
        self.variance = self.values['variance']
        self.std_deviation = self.values['std_deviation']


class ExtendedStats(Stats):
    """A multi-value metrics aggregation that computes stats over all extracted
    numeric values. See
    `extended_stats agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html>`_.

    This aggregation is an extended version of the :class:`Stats` aggregation.
    There are some additional metrics:
    `sum_of_squares`, `variance`, `std_deviation`.
    """  # noqa:E501
    __agg_name__ = 'extended_stats'

    result_cls = ExtendedStatsResult

    def __init__(self, field=None, script=None, **kwargs):
        super(ExtendedStats, self).__init__(
            field=field, script=script, **kwargs
        )


class BasePercentilesAggResult(MultiValueMetricsAggResult):
    def __init__(self, *args, **kwargs):
        super(BasePercentilesAggResult, self).__init__(*args, **kwargs)
        # TODO: Add support for keyed response
        values = []
        for k, v in self.values.items():
            # TODO: Do we need try-catch there?
            try:
                values.append((float(k), maybe_float(v)))
            except ValueError:
                pass
        self.values = sorted(values, key=lambda e: e[0])


class PercentilesAggResult(BasePercentilesAggResult):
    def get_value(self, percent):
        for p, v in self.values:
            if round(abs(p - percent), 7) == 0:
                return v


class Percentiles(MultiValueMetricsAgg):
    """A multi-value metrics aggregation that calculates percentiles over all
    extracted numeric values. See
    `percentiles agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html>`_.

    .. note::

       Percentiles are usually calculated approximately. Elasticsearch
       calculates them using
       `TDigest <https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf>`_
       algotighm.

    .. testsetup:: percentiles

       search_query = sq({'load_time_outlier': {'values': {
           '1.0': 15,
           '5.0': 20,
           '25.0': 23,
           '50.0': 25,
           '75.0': 29,
           '95.0': 60,
           '99.0': 150,
       }}})

    .. testcode:: percentiles

       search_query = search_query.aggs(
           load_time_outlier=agg.Percentiles(field=PageLoadDoc.load_time)
       )
       assert search_query.to_dict() == {
           'aggregations': {
               'load_time_outlier': {
                   'percentiles': {'field': 'load_time'}}}}
       load_time_agg = search_query.get_result() \
           .get_aggregation('load_time_outlier')
       for p, v in load_time_agg.values[:-1]:
           print('{:<4} - {}'.format(p, v))
       print('99 percentile is: {}'.format(load_time_agg.get_value(99)))

    .. testoutput:: percentiles

       1.0  - 15.0
       5.0  - 20.0
       25.0 - 23.0
       50.0 - 25.0
       75.0 - 29.0
       95.0 - 60.0
       99 percentile is: 150.0
    """  # noqa:E501
    __agg_name__ = 'percentiles'

    result_cls = PercentilesAggResult

    def __init__(
            self, field=None, script=None, percents=None, compression=None,
            **kwargs
    ):
        super(Percentiles, self).__init__(
            field=field, script=script, percents=percents,
            compression=compression, **kwargs
        )


class PercentileRanksAggResult(BasePercentilesAggResult):
    def get_percent(self, value):
        for v, p in self.values:
            if round(abs(v - value), 7) == 0:
                return p


class PercentileRanks(MultiValueMetricsAgg):
    """A multi-value metrics aggregation that calculates percentile ranks over
    all extracted numeric values. See
    `percentile_ranks agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html>`_.

    .. testsetup:: percentile-ranks

       search_query = sq({'load_time_outlier': {'values': {
           '15': 92,
           '30': 100,
       }}})

    .. testcode:: percentile-ranks

       search_query = search_query.aggs(
           load_time_outlier=agg.PercentileRanks(
               field=PageLoadDoc.load_time,
               values=[15, 30],
           )
       )
       assert search_query.to_dict() == {
           'aggregations': {
               'load_time_outlier': {
                   'percentile_ranks': {
                       'field': 'load_time',
                       'values': [15, 30]}}}}
       load_time_agg = search_query.get_result() \
           .get_aggregation('load_time_outlier')
       for v, p in load_time_agg.values:
           print('{:<4} - {}'.format(v, p))
       print(
           '{}% of values are below 15'.format(load_time_agg.get_percent(15))
       )

    .. testoutput:: percentile-ranks

       15.0 - 92.0
       30.0 - 100.0
       92.0% of values are below 15
    """  # noqa:E501
    __agg_name__ = 'percentile_ranks'

    result_cls = PercentileRanksAggResult

    def __init__(
            self, field=None, script=None, values=None, compression=None,
            **kwargs
    ):
        super(PercentileRanks, self).__init__(
            field=field, script=script, values=values, compression=compression,
            **kwargs
        )


class Cardinality(SingleValueMetricsAgg):
    """A single-value metrics aggregation that calculates an approximate count
    of distinct values. See
    `cardinality agg <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html>`_.
    """  # noqa:E501
    __agg_name__ = 'cardinality'

    def __init__(
            self, field=None, script=None, precision_threshold=None,
            rehash=None, **kwargs
    ):
        super(Cardinality, self).__init__(
            field=field, script=script,
            precision_threshold=precision_threshold, rehash=rehash,
            **kwargs
        )


class Bucket(object):
    _typed_key = True

    def __init__(
            self, raw_data, agg_expr, parent, doc_cls_map=None,
            mapper_registry=None,
    ):
        self.key = raw_data.get('key')
        if self._typed_key:
            self.key = agg_expr._type.to_python_single(self.key)
        self.doc_count = raw_data['doc_count']
        self.parent = parent
        self.aggregations = {}
        for agg_name, agg_expr in agg_expr._aggregations.items():
            result_agg = agg_expr.build_agg_result(
                raw_data[agg_name],
                doc_cls_map=doc_cls_map,
                mapper_registry=mapper_registry,
            )
            self.aggregations[agg_name] = result_agg

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    @cached_property
    def instance(self):
        self.parent._populate_instances()
        return self.__dict__['instance']

    def __repr__(self):
        return "<{} key={} doc_count={}>".format(
            self.__class__.__name__,
            self.key,
            self.doc_count
        )


class MultiBucketAggResult(AggResult):
    bucket_cls = Bucket

    def __init__(
            self, agg_expr, raw_data, doc_cls_map,
            mapper_registry, instance_mapper,
    ):
        super(MultiBucketAggResult, self).__init__(agg_expr)

        raw_buckets = raw_data.get('buckets', [])
        if isinstance(raw_buckets, dict):
            raw_buckets_map = raw_buckets
            raw_buckets = []
            for key, raw_bucket in sorted(
                    raw_buckets_map.items(), key=lambda i: i[0]
            ):
                raw_bucket = raw_bucket.copy()
                raw_bucket.setdefault('key', key)
                raw_buckets.append(raw_bucket)

        self._buckets = []
        self._buckets_map = {}
        for raw_bucket in raw_buckets:
            bucket = self.bucket_cls(
                raw_bucket, agg_expr, self, doc_cls_map=doc_cls_map,
                mapper_registry=mapper_registry,
            )
            self.add_bucket(bucket)

        self._instance_mapper = instance_mapper
        if mapper_registry is None:
            self._mapper_registry = {}
        else:
            self._mapper_registry = mapper_registry

        if self._instance_mapper:
            self._mapper_registry \
                .setdefault(self._instance_mapper, []) \
                .append(self)

    def add_bucket(self, bucket):
        self._buckets.append(bucket)
        if bucket.key is not None:
            self._buckets_map[bucket.key] = bucket

    def get_bucket(self, key):
        return self._buckets_map.get(key)

    @property
    def buckets(self):
        return list(self._buckets)

    def __iter__(self):
        return iter(self._buckets)

    def _populate_instances(self):
        buckets = list(chain(
            *(
                a._buckets for a in
                self._mapper_registry.get(self._instance_mapper, [self])
            )
        ))
        keys = [bucket.key for bucket in buckets]
        instances = (
            self._instance_mapper(keys) if self._instance_mapper else {}
        )
        for bucket in buckets:
            bucket.__dict__['instance'] = instances.get(bucket.key)


class MultiBucketAgg(BucketAgg):
    result_cls = MultiBucketAggResult

    def __init__(self, type=None, instance_mapper=None, **kwargs):
        super(MultiBucketAgg, self).__init__(**kwargs)
        self._type = instantiate(type or Type)
        self._instance_mapper = instance_mapper

    def clone(self):
        return self.__class__(
            aggs=self._aggregations,
            type=self._type,
            instance_mapper=self._instance_mapper,
            **self.params
        )

    def build_agg_result(
            self, raw_data, doc_cls_map=None, mapper_registry=None
    ):
        return self.result_cls(
            self, raw_data, doc_cls_map, mapper_registry, self._instance_mapper
        )


class Terms(MultiBucketAgg):
    __agg_name__ = 'terms'

    def __init__(
            self, field=None, script=None, size=None, shard_size=None,
            order=None, min_doc_count=None, shard_min_doc_count=None,
            include=None, exclude=None, collect_mode=None,
            execution_hint=None, type=None, instance_mapper=None, aggs=None,
            **kwargs
    ):
        type = type or (field.get_type() if field else None)
        super(Terms, self).__init__(
            field=field, script=script, size=size, shard_size=shard_size,
            order=order, min_doc_count=min_doc_count,
            shard_min_doc_count=shard_min_doc_count, include=include,
            exclude=exclude, collect_mode=collect_mode,
            execution_hint=execution_hint, type=type, aggs=aggs,
            **kwargs
        )
        self._instance_mapper = instance_mapper

    def clone(self):
        return self.__class__(
            type=self._type,
            aggs=self._aggregations,
            instance_mapper=self._instance_mapper,
            **self.params
        )


class SignificantTermsBucket(Bucket):
    def __init__(
            self, raw_data, aggs, parent,
            doc_cls_map=None, mapper_registry=None,
    ):
        super(SignificantTermsBucket, self).__init__(
            raw_data, aggs, parent,
            doc_cls_map=doc_cls_map, mapper_registry=mapper_registry,
        )
        self.score = raw_data['score']
        self.bg_count = raw_data['bg_count']


class SignificantTermsAggResult(MultiBucketAggResult):
    bucket_cls = SignificantTermsBucket


class SignificantTerms(Terms):
    __agg_name__ = 'significant_terms'

    result_cls = SignificantTermsAggResult


class Histogram(MultiBucketAgg):
    __agg_name__ = 'histogram'

    def __init__(
            self, field, interval=None, aggs=None, min_doc_count=1, **kwargs
    ):
        super(Histogram, self).__init__(
            field=field, interval=interval, min_doc_count=min_doc_count,
            aggs=aggs, **kwargs)


class DateHistogram(Histogram):
    __agg_name__ = 'date_histogram'


class RangeBucket(Bucket):
    _typed_key = False

    def __init__(
            self, raw_data, agg_expr, parent,
            doc_cls_map=None, mapper_registry=None,
    ):
        super(RangeBucket, self).__init__(
            raw_data, agg_expr, parent,
            doc_cls_map=doc_cls_map, mapper_registry=mapper_registry,
        )
        self.from_ = agg_expr._type.to_python_single(raw_data.get('from'))
        self.to = agg_expr._type.to_python_single(raw_data.get('to'))
        if self.key is None:
            self.key = (self.from_, self.to)


class RangeAggResult(MultiBucketAggResult):
    bucket_cls = RangeBucket


class Range(MultiBucketAgg):
    __agg_name__ = 'range'

    result_cls = RangeAggResult

    def __init__(
            self, field=None, script=None, ranges=None, type=None, aggs=None,
            **kwargs
    ):
        type = type or (field.get_type() if field else None)
        super(Range, self).__init__(
            field=field, script=script, ranges=ranges,
            type=type, aggs=aggs, **kwargs
        )


class Filters(MultiBucketAgg):
    __agg_name__ = 'filters'

    def __init__(self, filters, aggs=None, **kwargs):
        super(Filters, self).__init__(filters=filters, aggs=aggs, **kwargs)


class SingleBucketAggResult(AggResult):
    def __init__(self, agg_expr, raw_data, doc_cls_map, mapper_registry):
        super(SingleBucketAggResult, self).__init__(agg_expr)

        self.doc_count = raw_data.get('doc_count')
        self.aggregations = {}
        for agg_name, agg_expr in agg_expr._aggregations.items():
            agg_result = agg_expr.build_agg_result(
                raw_data.get(agg_name, {}),
                doc_cls_map=doc_cls_map,
                mapper_registry=mapper_registry,
            )
            self.aggregations[agg_name] = agg_result

    def get_aggregation(self, name):
        return self.aggregations.get(name)


class SingleBucketAgg(BucketAgg):
    result_cls = SingleBucketAggResult


class Global(SingleBucketAgg):
    __agg_name__ = 'global'


class Filter(SingleBucketAgg):
    __visit_name__ = 'filter_agg'
    __agg_name__ = 'filter'

    def __init__(self, filter, **kwargs):
        super(Filter, self).__init__(**kwargs)
        self.filter = filter

    def clone(self):
        return self.__class__(
            self.filter, aggs=self._aggregations, **self.params
        )


class Missing(SingleBucketAgg):
    __agg_name__ = 'missing'


class Nested(SingleBucketAgg):
    __agg_name__ = 'nested'

    def __init__(self, path, **kwargs):
        super(Nested, self).__init__(path=path, **kwargs)


class ReverseNested(SingleBucketAgg):
    __agg_name__ = 'reverse_nested'

    def __init__(self, path, **kwargs):
        super(ReverseNested, self).__init__(path=path, **kwargs)


class Sampler(SingleBucketAgg):
    __agg_name__ = 'sampler'

    def __init__(
            self, shard_size=None, field=None, script=None,
            execution_hint=None, **kwargs
    ):
        super(Sampler, self).__init__(
            shard_size=shard_size, field=field, script=script,
            execution_hint=execution_hint, **kwargs
        )


class BucketScript(SingleValueMetricsAgg):
    __agg_name__ = 'bucket_script'

    def __init__(
        self,
        buckets_path,
        script,
        gap_policy=None,
        format=None,
        **kwargs
    ):
        super(BucketScript, self).__init__(
            script=script,
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            format=format,
            **kwargs
        )


class AvgBucket(SingleValueMetricsAgg):
    __agg_name__ = 'avg_bucket'

    def __init__(
        self,
        buckets_path,
        gap_policy=None,
        format=None,
        **kwargs
    ):
        super(AvgBucket, self).__init__(
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            format=format,
            **kwargs)


class PercentilesBucket(MultiValueMetricsAgg):
    __agg_name__ = 'percentiles_bucket'

    result_cls = PercentilesAggResult

    def __init__(
        self,
        buckets_path,
        gap_policy=None,
        format=None,
        percents=None,
        **kwargs
    ):
        super(PercentilesBucket, self).__init__(
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            format=format,
            percents=percents,
            **kwargs)


class ScriptedMetric(SingleValueMetricsAgg):
    __agg_name__ = 'scripted_metric'

    def __init__(
        self,
        map_script,
        init_script=None,
        combine_script=None,
        reduce_script=None,
        **kwargs
    ):
        super(ScriptedMetric, self).__init__(
            map_script=map_script,
            init_script=init_script,
            combine_script=combine_script,
            reduce_script=reduce_script,
            **kwargs)
