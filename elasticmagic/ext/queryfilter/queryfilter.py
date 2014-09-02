import operator
from itertools import chain, groupby
from collections import defaultdict

from elasticmagic import Term, Terms, Query, And, Or, agg
from elasticmagic.types import String, instantiate

from .codec import SimpleCodec


def exact_op(f, values):
    if len(values) == 1:
        return f == values[0]
    return f.in_(values)


OPERATORS = {
    'exact': exact_op,
    'gte': operator.ge,
    'gt': operator.gt,
    'lte': operator.le,
    'lt': operator.lt,
    # 'isnull': isnull_op,
}


class QueryFilter(object):
    DEFAULT_NAME = 'qf'

    def __init__(self, name=None, codec=None):
        self.name = name or self.DEFAULT_NAME
        self.codec = codec or SimpleCodec()
        self.filters = []
        self._params = {}

    @property
    def _filter_types(self):
        types = {}
        for filt in self.filters:
            types[filt.name] = filt._types
        return types

    def add_filter(self, filter):
        self.filters.append(filter)

    def apply(self, search_query, params):
        self._params = self.codec.decode(params, self._filter_types)

        # First filter query
        for f in self.filters:
            search_query = f._apply_filter(search_query, self._params.get(f.name) or [])

        # disable all filters for aggregations
        if search_query._q:
            main_agg = agg.Filter(Query(search_query._q))
        else:
            main_agg = agg.Global()

        # then add aggregations
        for f in self.filters:
            main_agg = f._apply_agg(main_agg, search_query)

        if search_query._q:
            global_agg = agg.Global().aggs(**{self.name: main_agg})
        else:
            global_agg = main_agg
        return search_query.aggregations(**{self.name: global_agg})

    def process_results(self, results):
        global_agg = results.get_aggregation(self.name)
        main_agg = global_agg.get_aggregation(self.name)
        for f in self.filters:
            f._process_agg(main_agg, self._params.get(f.name) or [])

    def get_filter(self, name):
        for f in self.filters:
            if f.name == name:
                return f


class Facet(object):
    def __init__(self, name, field, type=None, instance_mapper=None):
        self.name = name
        self.field = field
        self.type = instantiate(type or String)
        self.instance_mapper = instance_mapper

        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    @property
    def _types(self):
        return [self.type]

    def _apply_filter(self, search_query, params):
        if not params:
            return search_query

        filts = []
        ops = defaultdict(list)
        for op, v in params:
            ops[op].append(v[0])

        for op, values in ops.items():
            op_func = OPERATORS.get(op)
            filts.append(op_func(self.field, values))

        if len(filts) == 1:
            filt = filts[0]
        else:
            filt = Or(*filts)
        return search_query.filter(filt, tags=[self.name])

    def _apply_agg(self, main_agg, search_query):
        filters = []
        for filt, meta in search_query._filters:
            tags = meta.get('tags') or set()
            if self.name not in tags:
                filters.append(filt)

        terms_agg = agg.Terms(self.field)
        if filters:
            main_agg = main_agg.aggs(
                **{self.name: agg.Filter(And(*filters), aggs={self.name: terms_agg})}
            )
        else:
            main_agg = main_agg.aggs(**{self.name: terms_agg})

        return main_agg
        
    def _process_agg(self, main_agg, params):
        values = set(chain(*(v for op, v in params if op == 'exact')))
        terms_agg = main_agg.get_aggregation(self.name)
        if terms_agg.get_aggregation(self.name):
            terms_agg = terms_agg.get_aggregation(self.name)
        for bucket in terms_agg.buckets:
            selected = bucket.key in values
            self.values.append(FacetValue(bucket, selected))


class FacetValue(object):
    def __init__(self, bucket, selected):
        self.bucket = bucket
        self.selected = selected

    @property
    def value(self):
        return self.bucket.key

    @property
    def count(self):
        return self.bucket.count
