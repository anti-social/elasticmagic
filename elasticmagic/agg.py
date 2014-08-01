from .expression import Expression, Params
from .util import _with_clone


class AggExpression(Expression):
    __visit_name__ = 'agg'


class MetricsAggExpression(AggExpression):
    def __init__(self, field=None, script=None, **kwargs):
        super(MetricsAggExpression, self).__init__(field=field, script=script, **kwargs)


class BucketAggExpression(AggExpression):
    __visit_name__ = 'bucket_agg'

    def __init__(self, **kwargs):
        super(BucketAggExpression, self).__init__(**kwargs)
        self._aggs = Params()

    def clone(self):
        cls = self.__class__
        obj = cls.__new__(cls)
        obj.__dict__ = self.__dict__.copy()
        return obj

    @_with_clone
    def aggs(self, **aggs):
        self._aggs = Params(aggs)
        

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

class Terms(BucketAggExpression):
    __agg_name__ = 'terms'

    def __init__(
            self, field=None, script=None, size=None, shard_size=None,
            order=None, min_doc_count=None, shard_min_doc_count=None,
            include=None, exclude=None, collect_mode=None,
            execution_hint=None, **kwargs
    ):
        super(Terms, self).__init__(
            field=field, script=script, size=size, shard_size=shard_size,
            order=order, min_doc_count=min_doc_count, shard_min_doc_count=shard_min_doc_count,
            include=include, exclude=exclude, collect_mode=collect_mode,
            execution_hint=execution_hint, **kwargs
        )


class Global(BucketAggExpression):
    __agg_name__ = 'global'


class Filter(BucketAggExpression):
    __agg_name__ = 'filter'


class TopHits(MetricsAggExpression):
    __agg_name__ = 'top_hits'

    def __init__(self, size=None, from_=None, sort=None, _source=None, **kwargs):
        super(TopHits, self).__init__(
            size=size, from_=from_, sort=sort, _source=_source, **kwargs
        )
