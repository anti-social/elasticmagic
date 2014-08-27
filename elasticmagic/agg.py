from .expression import Expression, Params
from .util import _with_clone


class AggExpression(Expression):
    __visit_name__ = 'agg'

    def clone(self):
        return self.__class__(**self.params)
    
    def process_results(self, raw_data):
        raise NotImplementedError()


class MetricsAgg(AggExpression):
    pass


class BucketAgg(AggExpression):
    __visit_name__ = 'bucket_agg'

    def __init__(self, aggs=None, **kwargs):
        super(BucketAgg, self).__init__(**kwargs)
        self._aggs = Params(aggs or {}, **kwargs.pop('aggregations', {}))

    def clone(self):
        return self.__class__(aggs=self._aggs, **self.params)


class SingleValueMetricsAgg(MetricsAgg):
    def __init__(self, field=None, script=None, **kwargs):
        super(SingleValueMetricsAgg, self).__init__(field=field, script=script, **kwargs)
        self.value = None

    def process_results(self, raw_data):
        self.value = raw_data['value']


class MultiValueMetricsAgg(MetricsAgg):
    def __init__(self, field=None, script=None, **kwargs):
        super(MultiValueMetricsAgg, self).__init__(field=field, script=script, **kwargs)
        self.values = {}

    def process_results(self, raw_data):
        if 'values' in raw_data:
            self.values = raw_data['values'].copy()
        else:
            self.values = raw_data.copy()


class Min(SingleValueMetricsAgg):
    __agg_name__ = 'min'


class Max(SingleValueMetricsAgg):
    __agg_name__ = 'max'


class Sum(SingleValueMetricsAgg):
    __agg_name__ = 'sum'


class Avg(SingleValueMetricsAgg):
    __agg_name__ = 'avg'


class TopHits(SingleValueMetricsAgg):
    __agg_name__ = 'top_hits'

    def __init__(self, size=None, from_=None, sort=None, _source=None, **kwargs):
        super(TopHits, self).__init__(
            size=size, from_=from_, sort=sort, _source=_source, **kwargs
        )


class Stats(MultiValueMetricsAgg):
    __agg_name__ = 'stats'

    def __init__(self, field=None, script=None, **kwargs):
        super(Stats, self).__init__(field=field, script=script, **kwargs)
        self.count = None
        self.min = None
        self.max = None
        self.avg = None
        self.sum = None

    def process_results(self, raw_data):
        super(Stats, self).process_results(raw_data)
        self.count = self.values['count']
        self.min = self.values['min']
        self.max = self.values['max']
        self.avg = self.values['avg']
        self.sum = self.values['sum']


class ExtendedStats(Stats):
    __agg_name__ = 'extended_stats'

    def __init__(self, field=None, script=None, **kwargs):
        super(ExtendedStats, self).__init__(field=field, script=script, **kwargs)
        self.sum_of_squares = None
        self.variance = None
        self.std_deviation = None

    def process_results(self, raw_data):
        super(ExtendedStats, self).process_results(raw_data)
        self.sum_of_squares = self.values['sum_of_squares']
        self.variance = self.values['variance']
        self.std_deviation = self.values['std_deviation']


class Percentiles(MultiValueMetricsAgg):
    __agg_name__ = 'percentiles'

    def __init__(self, field=None, script=None, percents=None, compression=None, **kwargs):
        super(Percentiles, self).__init__(
            field=field, script=script, percents=percents, compression=compression, **kwargs
        )


class PercentileRanks(MultiValueMetricsAgg):
    __agg_name__ = 'percentile_ranks'

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
    def __init__(self, raw_data, aggs):
        self.key = raw_data.get('key')
        self.doc_count = raw_data['doc_count']
        self.aggregations = {}
        for agg_name, agg in aggs.items():
            agg = agg.clone()
            agg.process_results(raw_data[agg_name])
            self.aggregations[agg_name] = agg

    def get_aggregation(self, name):
        return self.aggregations.get(name)


class MultiBucketAgg(BucketAgg):
    bucket_cls = Bucket

    def __init__(self, **kwargs):
        super(MultiBucketAgg, self).__init__(**kwargs)
        self.buckets = []

    def __iter__(self):
        return iter(self.buckets)

    def process_results(self, raw_data):
        raw_buckets = raw_data.get('buckets', [])
        if isinstance(raw_buckets, dict):
            raw_buckets_map = raw_buckets
            raw_buckets = []
            for key, raw_bucket in raw_buckets_map.items():
                raw_bucket = raw_bucket.copy()
                raw_bucket.setdefault('key', key)
                raw_buckets.append(raw_bucket)

        for raw_bucket in raw_buckets:
            bucket = self.bucket_cls(raw_bucket, self._aggs)
            self.buckets.append(bucket)


class Terms(MultiBucketAgg):
    __agg_name__ = 'terms'

    def __init__(
            self, field=None, script=None, size=None, shard_size=None,
            order=None, min_doc_count=None, shard_min_doc_count=None,
            include=None, exclude=None, collect_mode=None,
            execution_hint=None, aggs=None, **kwargs
    ):
        super(Terms, self).__init__(
            field=field, script=script, size=size, shard_size=shard_size,
            order=order, min_doc_count=min_doc_count, shard_min_doc_count=shard_min_doc_count,
            include=include, exclude=exclude, collect_mode=collect_mode,
            execution_hint=execution_hint, aggs=aggs, **kwargs
        )


class SignificantBucket(Bucket):
    def __init__(self, raw_data, aggs):
        super(SignificantBucket, self).__init__(raw_data, aggs)
        self.score = raw_data['score']
        self.bg_count = raw_data['bg_count']


class SignificantTerms(Terms):
    __agg_name__ = 'significant_terms'

    bucket_cls = SignificantBucket


class Histogram(MultiBucketAgg):
    __agg_name__ = 'histogram'

    def __init__(self, field, interval=None, aggs=None, **kwargs):
        super(Histogram, self).__init__(field=field, interval=interval, aggs=aggs, **kwargs)


class RangeBucket(Bucket):
    def __init__(self, key, doc_count, from_=None, to=None):
        super(RangeBucket, self).__init__(key, doc_count)
        self.from_ = from_
        self.to = to


class Range(MultiBucketAgg):
    __agg_name__ = 'range'

    bucket_cls = RangeBucket

    def __init__(self, field=None, script=None, ranges=None, aggs=None, **kwargs):
        super(Range, self).__init__(field=field, script=script, ranges=ranges, aggs=aggs, **kwargs)


class Filters(MultiBucketAgg):
    __agg_name__ = 'filters'

    def __init__(self, filters, aggs=None, **kwargs):
        super(Filters, self).__init__(filters=filters, aggs=aggs, **kwargs)


class SingleBucketAgg(BucketAgg):
    def __init__(self, **kwargs):
        super(SingleBucketAgg, self).__init__(**kwargs)
        self.doc_count = None
        self.aggregations = {}

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def process_results(self, raw_data):
        self.doc_count = raw_data.get('doc_count')
        for agg_name, agg in self._aggs.items():
            agg = agg.clone()
            agg.process_results(raw_data.get(agg_name, {}))
            self.aggregations[agg_name] = agg


class Global(SingleBucketAgg):
    __agg_name__ = 'global'


class Filter(SingleBucketAgg):
    __agg_name__ = 'filter'

    def __init__(self, filter, **kwargs):
        super(Filter, self).__init__(filter=filter, **kwargs)


class Missing(SingleBucketAgg):
    __agg_name__ = 'missing'


class Nested(SingleBucketAgg):
    __agg_name__ = 'nested'

    def __init__(self, path, **kwargs):
        super(Nested, self).__init__(path=path, **kwargs)
