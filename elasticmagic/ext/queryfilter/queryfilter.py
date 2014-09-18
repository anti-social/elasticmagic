from itertools import chain

from elasticmagic import Term, Terms, Query, And, Or, agg
from elasticmagic.types import String, instantiate

from .codec import SimpleCodec


def exact_op(f, values):
    if len(values) == 1:
        return f == values[0]
    return f.in_(values)


class QueryFilter(object):
    NAME = 'qf'

    def __init__(self, name=None, codec=None):
        self.name = name or self.NAME
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
        if global_agg.get_aggregation(self.name):
            main_agg = global_agg.get_aggregation(self.name)
        else:
            main_agg = global_agg

        for f in self.filters:
            f._process_agg(main_agg, self._params.get(f.name) or [])

    def get_filter(self, name):
        for f in self.filters:
            if f.name == name:
                return f


class BaseFilter(object):
    def _apply_filter(self, search_query, params):
        raise NotImplementedError()

    def _apply_agg(self, main_agg, search_query):
        raise NotImplementedError()

    def _process_agg(self, main_agg, params):
        pass


class FacetFilter(BaseFilter):
    def __init__(self, name, field, type=None, instance_mapper=None):
        self.name = name
        self.field = field
        self.type = instantiate(type or self.field._type)
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

        values = list(chain(*params))
        if len(values) == 1:
            expr = self.field == values[0]
        else:
            expr = self.field.in_(values)

        return search_query.filter(expr, tags=[self.name])

    def _apply_agg(self, main_agg, search_query):
        filters = []
        for filt, meta in search_query._filters:
            tags = meta.get('tags') or set()
            if self.name not in tags:
                filters.append(filt)

        terms_agg = agg.Terms(self.field, instance_mapper=self.instance_mapper)
        if filters:
            main_agg = main_agg.aggs(
                **{self.name: agg.Filter(And(*filters), aggs={self.name: terms_agg})}
            )
        else:
            main_agg = main_agg.aggs(**{self.name: terms_agg})

        return main_agg
        
    def _process_agg(self, main_agg, params):
        values = list(chain(*params))
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
        self.value = self.bucket.key
        self.count = self.bucket.doc_count

    @property
    def instance(self):
        return self.bucket.instance


class RangeFilter(BaseFilter):

    def __init__(self, name, field, type=None):
        self.name = name
        self.field = field
        self.type = instantiate(type or self.field._type)
        self.min = None
        self.max = None

        self._min_agg_name = '{}_min'.format(self.name)
        self._max_agg_name = '{}_max'.format(self.name)

    @property
    def _types(self):
        return [self.type]

    def _apply_filter(self, search_query, params):
        if not params:
            return search_query

        values = params[0][0]
        if isinstance(values, tuple):
            from_, to = values
        else:
            from_, to = values, values

        return search_query.filter(self.field.range(gte=from_, lte=to), tags=[self.name])

    def _apply_agg(self, main_agg, search_query):
        filters = []
        for filt, meta in search_query._filters:
            tags = meta.get('tags') or set()
            if self.name not in tags:
                filters.append(filt)

        stat_aggs = {
            self._min_agg_name: agg.Min(self.field),
            self._max_agg_name: agg.Max(self.field),
        }
        if filters:
            main_agg = main_agg.aggs(
                **{self.name: agg.Filter(And(*filters), aggs=stat_aggs)}
            )
        else:
            main_agg = main_agg.aggs(**stat_aggs)

        return main_agg

    def _process_agg(self, main_agg, params):
        if main_agg.get_aggregation(self.name):
            base_agg = main_agg.get_aggregation(self.name)
        else:
            base_agg = main_agg

        self.min = base_agg.get_aggregation(self._min_agg_name).value
        self.max = base_agg.get_aggregation(self._max_agg_name).value
