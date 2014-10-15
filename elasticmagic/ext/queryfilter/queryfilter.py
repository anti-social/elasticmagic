from itertools import chain

from elasticmagic import Params, Term, Terms, Query, And, Or, agg
from elasticmagic.types import String, instantiate
from elasticmagic.compat import text_type, string_types, with_metaclass

from .codec import SimpleCodec


def exact_op(f, values):
    if len(values) == 1:
        return f == values[0]
    return f.in_(values)


class QueryFilter(object):
    NAME = 'qf'

    def __init__(self, name=None, codec=None):
        self._name = name or self.NAME
        self._codec = codec or SimpleCodec()
        self._filters = []

        self._params = {}
        self._state = {}
        self._data = {}

        for filt_name in dir(self):
            unbound_filter = getattr(self, filt_name)
            if isinstance(unbound_filter, UnboundFilter):
                self.add_filter(unbound_filter.bind(filt_name))

        self.reset()

    @property
    def _filter_types(self):
        types = {}
        for filt in self._filters:
            types[filt.name] = filt._types
        return types

    def _set_selected(self, name, value):
        self._state.setdefault(name, {})[value] = True

    def _selected(self, name, value):
        return self._state.get(name, {}).get(value, False)

    def _set_value_data(self, name, value, data):
        self._data.setdefault(name, {})[value] = data

    def _value_data(self, name, value):
        return self._data.get(name, {}).get(value, {})

    def reset(self):
        self._params = {}
        self._state = {}
        self._data = {}
        for filt in self._filters:
            filt._reset()

    @property
    def filters(self):
        return self._filters

    def add_filter(self, filter):
        filter.qf = self
        self._filters.append(filter)
        setattr(self, filter.name, filter)

    def apply(self, search_query, params):
        self._params = self._codec.decode(params, self._filter_types)

        # First filter query
        for f in self._filters:
            search_query = f._apply_filter(search_query, self._params.get(f.name) or {})

        # disable all filters for aggregations
        if search_query._q:
            main_agg = agg.Filter(Query(search_query._q))
        else:
            main_agg = agg.Global()

        # then add aggregations
        for f in self._filters:
            main_agg = f._apply_agg(main_agg, search_query)

        if search_query._q:
            global_agg = agg.Global().aggs(**{self._name: main_agg})
        else:
            global_agg = main_agg
        return search_query.aggregations(**{self._name: global_agg})

    def process_results(self, results):
        global_agg = results.get_aggregation(self._name)
        if global_agg.get_aggregation(self._name):
            main_agg = global_agg.get_aggregation(self._name)
        else:
            main_agg = global_agg

        for f in self._filters:
            f._process_agg(main_agg, self._params.get(f.name) or {})

    def get_filter(self, name):
        return getattr(self, name, None)


class UnboundFilter(object):
    def __init__(self, filter_cls, args, kwargs):
        self.filter_cls = filter_cls
        self.args = args
        self.kwargs = kwargs

    def bind(self, name):
        return self.filter_cls(name, *self.args, **self.kwargs)


class BaseFilter(object):
    def __new__(cls, *args, **kwargs):
        if args and not isinstance(args[0], string_types):
            return UnboundFilter(cls, args, kwargs)
        return super(BaseFilter, cls).__new__(cls, *args, **kwargs)

    def __init__(self, name):
        self.name = name
        self.qf = None

    def _reset(self):
        pass

    @property
    def _types(self):
        return []

    def _apply_filter(self, search_query, params):
        raise NotImplementedError()

    def _apply_agg(self, main_agg, search_query):
        raise NotImplementedError()

    def _process_agg(self, main_agg, params):
        pass


class FieldFilter(BaseFilter):
    def __init__(self, name, field):
        super(FieldFilter, self).__init__(name)
        self.field = field


class BaseFilterValue(object):
    def __init__(self, value):
        self.value = value
        self.filter = None

    def bind(self, filter):
        self.filter = filter
        return self

    @property
    def data(self):
        return self.filter.qf._value_data(self.filter.name, self.value)

    @property
    def selected(self):
        return self.filter.qf._selected(self.filter.name, self.value)


class FacetFilter(FieldFilter):
    def __init__(self, name, field, type=None, instance_mapper=None, **kwargs):
        super(FacetFilter, self).__init__(name, field)
        self.type = instantiate(type or self.field._type)
        self.instance_mapper = instance_mapper
        self.agg_kwargs = kwargs

        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

        self._filter_agg_name = '{}.filter'.format

    def _reset(self):
        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    @property
    def _types(self):
        return [self.type]

    def _apply_filter(self, search_query, params):
        values = params.get('exact')
        if not values:
            return search_query

        values = list(chain(*values))
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

        terms_agg = agg.Terms(self.field, instance_mapper=self.instance_mapper, **self.agg_kwargs)
        if filters:
            main_agg = main_agg.aggs(
                **{self._filter_agg_name(self.name): agg.Filter(And(*filters), aggs={self.name: terms_agg})}
            )
        else:
            main_agg = main_agg.aggs(**{self.name: terms_agg})

        return main_agg
        
    def _process_agg(self, main_agg, params):
        values = params.get('exact', [])
        values = list(chain(*values))
        if main_agg.get_aggregation(self._filter_agg_name(self.name)):
            terms_agg = main_agg \
                .get_aggregation(self._filter_agg_name(self.name)) \
                .get_aggregation(self.name)
        else:
            terms_agg = main_agg.get_aggregation(self.name)
        for bucket in terms_agg.buckets:
            if bucket.key in values:
                self.qf._set_selected(self.name, bucket.key)
            self.qf._set_value_data(self.name, bucket.key, {'bucket': bucket})
            self.add_value(FacetValue(bucket.key).bind(self))

    def add_value(self, fv):
        self.all_values.append(fv)
        self.values_map[fv.value] = fv
        if fv.selected:
            self.selected_values.append(fv)
        else:
            self.values.append(fv)

    def get_value(self, value):
        return self.values_map.get(value)


class FacetValue(BaseFilterValue):
    @property
    def bucket(self):
        return self.data.get('bucket')

    @property
    def count(self):
        bucket = self.bucket
        if bucket:
            return self.bucket.doc_count

    @property
    def instance(self):
        bucket = self.bucket
        if bucket:
            return self.bucket.instance

    @property
    def filter_name(self):
        return self.filter.name

    @property
    def filter_value(self):
        return self.filter.qf._codec.encode_value(self.value)

    @property
    def count_text(self):
        return text_type(self.count)

    @property
    def title(self):
        if self.instance:
            return text_type(self.instance)
        return text_type(self.value)

    def __unicode__(self):
        return self.title


class RangeFilter(FieldFilter):

    def __init__(self, name, field, type=None):
        super(RangeFilter, self).__init__(name, field)
        self.type = instantiate(type or self.field._type)
        self.from_value = None
        self.to_value = None
        self.min = None
        self.max = None

        self._filter_agg_name = '{}.filter'.format
        self._min_agg_name = '{}.min'.format
        self._max_agg_name = '{}.max'.format

    def _reset(self):
        self.from_value = None
        self.to_value = None
        self.min = None
        self.max = None

    @property
    def _types(self):
        return [self.type]

    def _get_from_value(self, params):
        from_values = params.get('gte')
        return from_values[0][0] if from_values else None

    def _get_to_value(self, params):
        to_values = params.get('lte')
        return to_values[0][0] if to_values else None

    def _apply_filter(self, search_query, params):
        self.from_value = self._get_from_value(params)
        self.to_value = self._get_to_value(params)
        if self.from_value is None and self.to_value is None:
            return search_query

        return search_query.filter(
            self.field.range(gte=self.from_value, lte=self.to_value),
            tags=[self.name]
        )

    def _apply_agg(self, main_agg, search_query):
        filters = []
        for filt, meta in search_query._filters:
            tags = meta.get('tags') or set()
            if self.name not in tags:
                filters.append(filt)

        stat_aggs = {
            self._min_agg_name(self.name): agg.Min(self.field),
            self._max_agg_name(self.name): agg.Max(self.field),
        }
        if filters:
            main_agg = main_agg.aggs(
                **{self._filter_agg_name(self.name): agg.Filter(And(*filters), aggs=stat_aggs)}
            )
        else:
            main_agg = main_agg.aggs(**stat_aggs)

        return main_agg

    def _process_agg(self, main_agg, params):
        if main_agg.get_aggregation(self._filter_agg_name(self.name)):
            base_agg = main_agg.get_aggregation(self._filter_agg_name(self.name))
        else:
            base_agg = main_agg

        self.min = base_agg.get_aggregation(self._min_agg_name(self.name)).value
        self.max = base_agg.get_aggregation(self._max_agg_name(self.name)).value


class FacetQueryValue(BaseFilterValue):
    def __init__(self, value, expr, **opts):
        super(FacetQueryValue, self).__init__(value)
        self.expr = expr
        self.opts = opts

    @property
    def agg(self):
        return self.data.get('agg')

    @property
    def count(self):
        agg = self.agg
        if agg:
            return self.agg.doc_count

    @property
    def count_text(self):
        if not self.count:
            return ''
        return '{}'.format(self.count)

    @property
    def filter_name(self):
        return self.filter.name

    @property
    def filter_value(self):
        return self.value

    @property
    def title(self):
        return text_type(self.opts.get('title', self.value))

    def __unicode__(self):
        return self.title


class FacetQueryFilter(BaseFilter):
    def __init__(self, name, *values, **agg_kwargs):
        super(FacetQueryFilter, self).__init__(name)
        self._values = values
        for fv in self._values:
            fv.bind(self)
        self.values_map = {fv.value: fv for fv in self._values}
        self.agg_kwargs = agg_kwargs

        self._filter_agg_name = '{}.filter'.format
        self._agg_name = '{}:{}'.format

    @property
    def all_values(self):
        return self._values

    @property
    def selected_values(self):
        return [fv for fv in self._values if fv.selected]

    @property
    def values(self):
        return [fv for fv in self._values if not fv.selected]

    def get_value(self, value):
        return self.values_map.get(value)

    def _apply_filter(self, search_query, params):
        values = params.get('exact')
        if not values:
            return search_query

        expressions = []
        for v in values:
            w = v[0]
            filter_value = self.get_value(w)
            if filter_value:
                expressions.append(filter_value.expr)

        if expressions:
            return search_query.filter(Or(*expressions), tags=[self.name])
        return search_query

    def _apply_agg(self, main_agg, search_query):
        filters = []
        for filt, meta in search_query._filters:
            tags = meta.get('tags') or set()
            if self.name not in tags:
                filters.append(filt)

        filter_aggs_map = {}
        for fv in self.values:
            filter_aggs_map[self._agg_name(self.name, fv.value)] = agg.Filter(fv.expr, **self.agg_kwargs)

        if filters:
            main_agg = main_agg.aggs(
                **{
                    self._filter_agg_name(self.name): agg.Filter(
                        And(*filters), aggs=filter_aggs_map
                    )
                }
            )
        else:
            main_agg = main_agg.aggs(**filter_aggs_map)

        return main_agg

    def _process_agg(self, main_agg, params):
        values = params.get('exact', [])
        values = list(chain(*values))
        if main_agg.get_aggregation(self._filter_agg_name(self.name)):
            filters_agg = main_agg.get_aggregation(self._filter_agg_name(self.name))
        else:
            filters_agg = main_agg
        for fv in self.values:
            filt_agg = filters_agg.get_aggregation(self._agg_name(self.name, fv.value))
            if fv.value in values:
                self.qf._set_selected(self.name, fv.value)
            self.qf._set_value_data(self.name, fv.value, {'agg': filt_agg})


class OrderingValue(BaseFilterValue):
    def __init__(self, value, orderings, **kwargs):
        super(OrderingValue, self).__init__(value)
        self.orderings = orderings
        self.opts = kwargs

    def __unicode__(self):
        return unicode(self.opts.get('title', self.value))

    def _apply(self, query):
        return query.order_by(*self.orderings)


class OrderingFilter(BaseFilter):
    def __init__(self, name, *values, **kwargs):
        super(OrderingFilter, self).__init__(name)
        self.values = values
        for ordering_value in self.values:
            ordering_value.filter = self
        self.default_value = self.get_value(kwargs.get('default')) or self.values[0]
        self.selected_value = None

    def get_value(self, value):
        for ordering_value in self.values:
            if ordering_value.value == value:
                return ordering_value

    def _reset(self):
        self.selected_value = None

    def _apply_filter(self, search_query, params):
        values = params.get('exact')

        ordering_value = None
        if values:
            ordering_value = self.get_value(values[0][0])
        if not ordering_value:
            ordering_value = self.default_value

        self.selected_value = ordering_value
        self.qf._set_selected(self.name, ordering_value.value)
        return ordering_value._apply(search_query)

    def _apply_agg(self, main_agg, search_query):
        return main_agg
