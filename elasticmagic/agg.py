from .expression import Expression, Params
from .util import _with_clone


class AggExpression(Expression):
    __visit_name__ = 'agg'

    def clone(self):
        return self.__class__(**self.params)
    
    def process_results(self, raw_data):
        raise NotImplementedError()


class MetricsAggExpression(AggExpression):
    def __init__(self, field=None, script=None, **kwargs):
        super(MetricsAggExpression, self).__init__(field=field, script=script, **kwargs)
        self.value = None

    def process_results(self, raw_data):
        self.value = raw_data['value']


class BucketAggExpression(AggExpression):
    __visit_name__ = 'bucket_agg'

    def __init__(self, aggs=None, **kwargs):
        super(BucketAggExpression, self).__init__(**kwargs)
        self._aggs = Params(aggs or {}, **kwargs.pop('aggregations', {}))

    def clone(self):
        return self.__class__(aggs=self._aggs, **self.params)


class Min(MetricsAggExpression):
    __agg_name__ = 'min'


class Max(MetricsAggExpression):
    __agg_name__ = 'max'


class Sum(MetricsAggExpression):
    __agg_name__ = 'sum'


class Avg(MetricsAggExpression):
    __agg_name__ = 'avg'


class Percentiles(MetricsAggExpression):
    __agg_name__ = 'percentiles'

    def __init__(self, field=None, script=None, percents=None, **kwargs):
        super(Percentiles, self).__init__(
            field=field, script=script, percents=percents, **kwargs
        )


class Bucket(object):
    def __init__(self, raw_data, aggs):
        self.key = raw_data['key']
        self.doc_count = raw_data['doc_count']
        self.aggregations = {}
        for agg_name, agg in aggs.items():
            agg = agg.clone()
            agg.process_results(raw_data[agg_name])
            self.aggregations[agg_name] = agg

    def get_aggregation(self, name):
        return self.aggregations.get(name)


class MultiBucketAgg(BucketAggExpression):
    bucket_cls = Bucket

    def __init__(self, **kwargs):
        super(MultiBucketAgg, self).__init__(**kwargs)
        self.buckets = []

    def __iter__(self):
        return iter(self.buckets)

    def process_results(self, raw_data):
        for raw_bucket in raw_data.get('buckets', []):
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


class SingleBucketAgg(BucketAggExpression):
    def __init__(self, filter=None, **kwargs):
        super(SingleBucketAgg, self).__init__(filter=filter, **kwargs)
        self.doc_count = None
        self.aggregations = {}

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def process_results(self, raw_data):
        # super(SingleBucketAgg, self).process_results(raw_data)
        self.doc_count = raw_data['doc_count']
        for agg_name, agg in self._aggs.items():
            agg = agg.clone()
            agg.process_results(raw_data.get(agg_name, {}))
            self.aggregations[agg_name] = agg


class Global(SingleBucketAgg):
    __agg_name__ = 'global'


class Filter(SingleBucketAgg):
    __agg_name__ = 'filter'


class TopHits(MetricsAggExpression):
    __agg_name__ = 'top_hits'

    def __init__(self, size=None, from_=None, sort=None, _source=None, **kwargs):
        super(TopHits, self).__init__(
            size=size, from_=from_, sort=sort, _source=_source, **kwargs
        )
