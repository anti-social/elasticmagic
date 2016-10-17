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

    def build_agg_result(self, raw_data, doc_cls_map=None, mapper_registry=None):
        raise NotImplementedError()


class AggResult(object):
    def __init__(self, agg_expr):
        self.expr = agg_expr


class MetricsAgg(AggExpression):
    def build_agg_result(self, raw_data, doc_cls_map=None, mapper_registry=None):
        return self.result_cls(self, raw_data)


class BucketAgg(AggExpression):
    __visit_name__ = 'bucket_agg'

    def __init__(self, aggs=None, **kwargs):
        super(BucketAgg, self).__init__(**kwargs)
        self._aggregations = Params(aggs or {}, **kwargs.pop('aggregations', {}))

    def clone(self):
        return self.__class__(aggs=self._aggregations, **self.params)

    @_with_clone
    def aggregations(self, *args, **kwargs):
        if len(args) == 1 and args[0] is None:
            self._aggregations = Params()
        else:
            self._aggregations = merge_params(self._aggregations, args, kwargs)

    aggs = aggregations

    def build_agg_result(self, raw_data, doc_cls_map=None, mapper_registry=None):
        return self.result_cls(self, raw_data, doc_cls_map=doc_cls_map, mapper_registry=mapper_registry)


class SingleValueMetricsAggResult(AggResult):
    def __init__(self, agg_expr, raw_data):
        super(SingleValueMetricsAggResult, self).__init__(agg_expr)
        self.value = maybe_float(raw_data['value'])
        self.value_as_string = raw_data.get('value_as_string', force_unicode(raw_data['value']))


class SingleValueMetricsAgg(MetricsAgg):
    result_cls = SingleValueMetricsAggResult
    
    def __init__(self, field=None, script=None, **kwargs):
        super(SingleValueMetricsAgg, self).__init__(field=field, script=script, **kwargs)


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
        super(MultiValueMetricsAgg, self).__init__(field=field, script=script, **kwargs)


class Min(SingleValueMetricsAgg):
    __agg_name__ = 'min'


class Max(SingleValueMetricsAgg):
    __agg_name__ = 'max'


class Sum(SingleValueMetricsAgg):
    __agg_name__ = 'sum'


class Avg(SingleValueMetricsAgg):
    __agg_name__ = 'avg'


class TopHitsResult(AggResult):
    def __init__(self, agg_expr, raw_data, doc_cls_map, mapper_registry, instance_mapper):
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
            self._instance_mappers = {doc_cls: instance_mapper for doc_cls in doc_cls_map.values()}

        if mapper_registry is None:
            self._mapper_registry = {}
        else:
            self._mapper_registry = mapper_registry

        if self._instance_mappers:
            for instance_mapper in self._instance_mappers.values():
                self._mapper_registry.setdefault(instance_mapper, []).append(self)

    def _populate_instances(self, doc_cls):
        instance_mapper = self._instance_mappers.get(doc_cls)
        hits = list(chain(
            *(
                map(
                    lambda r: filter(lambda hit: isinstance(hit, doc_cls), r.hits),
                    self._mapper_registry.get(instance_mapper, [self])
                )
            )
        ))
        ids = [hit._id for hit in hits]
        instances = instance_mapper(ids) if instance_mapper else {}
        for hit in hits:
            hit.__dict__['instance'] = instances.get(hit._id)


class TopHits(MetricsAgg):
    __agg_name__ = 'top_hits'

    result_cls = TopHitsResult

    def __init__(self, size=None, from_=None, sort=None, _source=None, instance_mapper=None, **kwargs):
        super(TopHits, self).__init__(
            size=size, from_=from_, sort=sort, _source=_source, **kwargs
        )
        self._instance_mapper = instance_mapper

    def build_agg_result(self, raw_data, doc_cls_map=None, mapper_registry=None):
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
    __agg_name__ = 'extended_stats'

    result_cls = ExtendedStatsResult

    def __init__(self, field=None, script=None, **kwargs):
        super(ExtendedStats, self).__init__(field=field, script=script, **kwargs)


class BasePercentilesAggResult(MultiValueMetricsAggResult):
    def __init__(self, *args, **kwargs):
        super(BasePercentilesAggResult, self).__init__(*args, **kwargs)
        values = []
        for k, v in self.values.items():
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
    __agg_name__ = 'percentiles'

    result_cls = PercentilesAggResult

    def __init__(self, field=None, script=None, percents=None, compression=None, **kwargs):
        super(Percentiles, self).__init__(
            field=field, script=script, percents=percents, compression=compression, **kwargs
        )


class PercentileRanksAggResult(BasePercentilesAggResult):
    def get_percent(self, value):
        for v, p in self.values:
            if round(abs(v - value), 7) == 0:
                return p


class PercentileRanks(MultiValueMetricsAgg):
    __agg_name__ = 'percentile_ranks'

    result_cls = PercentileRanksAggResult

    def __init__(self, field=None, script=None, values=None, compression=None, **kwargs):
        super(PercentileRanks, self).__init__(
            field=field, script=script, values=values, compression=None, **kwargs
        )


class Cardinality(SingleValueMetricsAgg):
    __agg_name__ = 'cardinality'

    def __init__(self, field=None, script=None, precision_threshold=None, rehash=None, **kwargs):
        super(Cardinality, self).__init__(
            field=field, script=script,
            precision_threshold=precision_threshold, rehash=rehash,
            **kwargs
        )


class Bucket(object):
    _typed_key = True

    def __init__(self, raw_data, agg_expr, parent, doc_cls_map=None, mapper_registry=None):
        self.key = raw_data.get('key')
        if self._typed_key:
            self.key = agg_expr._type.to_python_single(self.key)
        self.doc_count = raw_data['doc_count']
        self.parent = parent
        self.aggregations = {}
        for agg_name, agg_expr in agg_expr._aggregations.items():
            result_agg = agg_expr.build_agg_result(
                raw_data[agg_name], doc_cls_map=doc_cls_map, mapper_registry=mapper_registry
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

    def __init__(self, agg_expr, raw_data, doc_cls_map, mapper_registry, instance_mapper):
        super(MultiBucketAggResult, self).__init__(agg_expr)

        raw_buckets = raw_data.get('buckets', [])
        if isinstance(raw_buckets, dict):
            raw_buckets_map = raw_buckets
            raw_buckets = []
            for key, raw_bucket in sorted(raw_buckets_map.items(), key=lambda i: i[0]):
                raw_bucket = raw_bucket.copy()
                raw_bucket.setdefault('key', key)
                raw_buckets.append(raw_bucket)

        self._buckets = []
        self._buckets_map = {}
        for raw_bucket in raw_buckets:
            bucket = self.bucket_cls(
                raw_bucket, agg_expr, self, doc_cls_map=doc_cls_map, mapper_registry=mapper_registry
            )
            self.add_bucket(bucket)

        self._instance_mapper = instance_mapper
        if mapper_registry is None:
            self._mapper_registry = {}
        else:
            self._mapper_registry = mapper_registry

        if self._instance_mapper:
            self._mapper_registry.setdefault(self._instance_mapper, []).append(self)

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
            *(a._buckets for a in self._mapper_registry.get(self._instance_mapper, [self]))
        ))
        keys = [bucket.key for bucket in buckets]
        instances = self._instance_mapper(keys) if self._instance_mapper else {}
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

    def build_agg_result(self, raw_data, doc_cls_map=None, mapper_registry=None):
        return self.result_cls(self, raw_data, doc_cls_map, mapper_registry, self._instance_mapper)


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
            order=order, min_doc_count=min_doc_count, shard_min_doc_count=shard_min_doc_count,
            include=include, exclude=exclude, collect_mode=collect_mode,
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
    def __init__(self, raw_data, aggs, parent, doc_cls_map=None, mapper_registry=None):
        super(SignificantTermsBucket, self).__init__(
            raw_data, aggs, parent, doc_cls_map=doc_cls_map, mapper_registry=mapper_registry
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

    def __init__(self, field, interval=None, aggs=None, **kwargs):
        super(Histogram, self).__init__(field=field, interval=interval, aggs=aggs, **kwargs)


class RangeBucket(Bucket):
    _typed_key = False

    def __init__(self, raw_data, agg_expr, parent, doc_cls_map=None, mapper_registry=None):
        super(RangeBucket, self).__init__(raw_data, agg_expr, parent, doc_cls_map=doc_cls_map, mapper_registry=mapper_registry)
        self.from_ = agg_expr._type.to_python_single(raw_data.get('from'))
        self.to = agg_expr._type.to_python_single(raw_data.get('to'))
        if self.key is None:
            self.key = (self.from_, self.to)


class RangeAggResult(MultiBucketAggResult):
    bucket_cls = RangeBucket


class Range(MultiBucketAgg):
    __agg_name__ = 'range'

    result_cls = RangeAggResult

    def __init__(self, field=None, script=None, ranges=None, type=None, aggs=None, **kwargs):
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
                raw_data.get(agg_name, {}), doc_cls_map=doc_cls_map, mapper_registry=mapper_registry
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
        return self.__class__(self.filter, aggs=self._aggregations, **self.params)


class Missing(SingleBucketAgg):
    __agg_name__ = 'missing'


class Nested(SingleBucketAgg):
    __agg_name__ = 'nested'

    def __init__(self, path, **kwargs):
        super(Nested, self).__init__(path=path, **kwargs)


class Sampler(SingleBucketAgg):
    __agg_name__ = 'sampler'

    def __init__(self, shard_size=None, field=None, script=None, execution_hint=None, **kwargs):
        super(Sampler, self).__init__(
            shard_size=shard_size, field=field, script=script, execution_hint=execution_hint,
            **kwargs
        )
