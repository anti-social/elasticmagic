import functools
import logging
import operator
from math import ceil
from itertools import chain

from elasticmagic import agg
from elasticmagic.cluster import MAX_RESULT_WINDOW
from elasticmagic.compat import text_type, string_types, with_metaclass
from elasticmagic.expression import Bool, MatchAll, Nested
from elasticmagic.types import Integer, instantiate

from .codec import SimpleCodec


log = logging.getLogger(__name__)

is_not_none = functools.partial(operator.is_not, None)


class UnboundFilter(object):
    _current_counter = 0

    def __init__(self, filter_cls, args, kwargs):
        self.filter_cls = filter_cls
        self.args = args
        self.kwargs = kwargs
        self._counter = UnboundFilter._current_counter
        UnboundFilter._current_counter += 1

    def bind(self, name):
        return self.filter_cls(name, *self.args, **self.kwargs)


class QueryFilterMeta(type):
    def __init__(cls, name, bases, attrs):
        type.__init__(cls, name, bases, attrs)

        cls._unbound_filters = []
        for attr_name, attr in attrs.items():
            if isinstance(attr, UnboundFilter):
                cls._unbound_filters.append((attr_name, attr))
                delattr(cls, attr_name)

        cls._unbound_filters.sort(key=lambda e: e[1]._counter)

    def __setattr__(cls, name, value):
        if isinstance(value, UnboundFilter):
            cls._unbound_filters.append((name, value))
        else:
            type.__setattr__(cls, name, value)


class QueryFilter(with_metaclass(QueryFilterMeta)):
    NAME = 'qf'

    CONJ_OR = 'CONJ_OR'
    CONJ_AND = 'CONJ_AND'

    def __init__(self, name=None, codec=None):
        self._name = name or self.NAME
        self._codec = codec or SimpleCodec()
        self._filters = []

        self._params = {}
        self._state = {}
        self._data = {}

        for base_cls in reversed(self.__class__.__mro__):
            if hasattr(base_cls, '_unbound_filters'):
                for filter_name, unbound_filter in base_cls._unbound_filters:
                    self.add_filter(unbound_filter.bind(filter_name))

        self.reset()

    def get_name(self):
        return self._name

    def get_types(self):
        types = {}
        for filt in self._filters:
            types.update(filt._types)
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
        self.remove_filter(filter.name)
        filter.qf = self
        self._filters.append(filter)
        setattr(self, filter.name, filter)

    def remove_filter(self, filter_name):
        if isinstance(getattr(self, filter_name, None), BaseFilter):
            delattr(self, filter_name)
            for ix, f in enumerate(self._filters):
                if f.name == filter_name:
                    break
            self._filters = self._filters[:ix] + self._filters[ix + 1:]

    def apply(self, search_query, params):
        self._params = self._codec.decode(params, self.get_types())

        # First filter query with all filters
        for f in self._filters:
            search_query = f._apply_filter(search_query, self._params)

        # then add aggregations
        for f in self._filters:
            search_query = f._apply_agg(search_query)

        return search_query

    def process_result(self, result):
        filter_results = {}
        for f in self._filters:
            filter_results[f.name] = f._process_result(
               result, self._params)
        return QueryFilterResult(filter_results)

    process_results = process_result

    def get_filter(self, name):
        return getattr(self, name, None)


class QueryFilterResult(object):
    def __init__(self, filters):
        self._filters = filters
        for filter_name, filter_result in self._filters.items():
            setattr(self, filter_name, filter_result)

    @property
    def filters(self):
        return self._filters

    def get_filter(self, name):
        return self._filters.get(name)


class BaseFilterResult(object):
    def __init__(self, name, alias):
        self.name = name
        self.alias = alias


class BaseFilter(object):
    def __new__(cls, *args, **kwargs):
        if not args or not isinstance(args[0], string_types):
            return UnboundFilter(cls, args, kwargs)
        return super(BaseFilter, cls).__new__(cls)

    def __init__(self, name, alias=None):
        self.name = name
        self.alias = alias or self.name
        self.qf = None

    def _reset(self):
        pass

    @property
    def _types(self):
        return {}

    def _get_agg_filters(self, filters, exclude_tags):
        active_filters = []
        for filt, meta in filters:
            tags = meta.get('tags', set()) if meta else set()
            if not exclude_tags.intersection(tags):
                active_filters.append(filt)
        return active_filters

    def _apply_filter(self, search_query, params):
        raise NotImplementedError()

    def _apply_agg(self, search_query):
        return search_query

    def _process_result(self, result, params):
        return BaseFilterResult(self.name, self.alias)


class FieldFilter(BaseFilter):
    def __init__(self, name, field, alias=None, type=None):
        super(FieldFilter, self).__init__(name, alias=alias)
        self.field = field
        self.type = instantiate(type or self.field.get_type())

    @property
    def _types(self):
        return {self.alias: self.type}


class BaseFilterValue(object):
    def __init__(self, value, _filter=None):
        self.value = value
        self.filter = _filter

    def bind(self, filter):
        return self.__class__(self.value, _filter=filter)

    @property
    def data(self):
        return self.filter.qf._value_data(self.filter.name, self.value)

    @property
    def selected(self):
        return self.filter.qf._selected(self.filter.name, self.value)


class SimpleFilter(FieldFilter):
    def __init__(self, name, field, alias=None, type=None,
                 conj_operator=QueryFilter.CONJ_OR, allow_null=False):
        super(SimpleFilter, self).__init__(name, field, alias=alias, type=type)
        self._conj_operator = conj_operator
        self._allow_null = allow_null

    def _get_values_from_params(self, params):
        values = params.get('exact', [])
        if not self._allow_null:
            values = filter(is_not_none, values)
        return list(values)

    def _get_expression(self, params):
        values = self._get_values_from_params(params.get(self.alias, {}))
        if not values:
            return None

        if len(values) == 1:
            return self.field == values[0]

        if self._conj_operator == QueryFilter.CONJ_AND:
            return Bool.must(*(self.field == v for v in values))
        elif all(map(is_not_none, values)):
            return self.field.in_(values)
        else:
            return Bool.should(*(self.field == v for v in values))

    def _apply_filter(self, search_query, params):
        expr = self._get_expression(params)
        if expr is None:
            return search_query
        return search_query.filter(expr, meta={'tags': {self.name}})


class FacetFilter(SimpleFilter):
    def __init__(
            self, name, field, alias=None, type=None,
            conj_operator=QueryFilter.CONJ_OR, instance_mapper=None,
            get_title=None, filters=None, **kwargs
    ):
        super(FacetFilter, self).__init__(
            name, field, alias=alias, type=type, conj_operator=conj_operator
        )
        self._allow_null = False
        self._instance_mapper = instance_mapper
        self._get_title = get_title
        self._filters = filters
        self._agg_kwargs = kwargs

        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    def _reset(self):
        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    @property
    def _agg_name(self):
        return '{}.{}'.format(self.qf._name, self.name)

    @property
    def _filter_agg_name(self):
        return '{}.{}.filter'.format(self.qf._name, self.name)

    def _apply_filter(self, search_query, params):
        expr = self._get_expression(params)
        if expr is None:
            return search_query
        return search_query.post_filter(expr, meta={'tags': {self.name}})

    def _apply_agg(self, search_query):
        exclude_tags = {self.qf._name}
        if self._conj_operator == QueryFilter.CONJ_OR:
            exclude_tags.add(self.name)
        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            exclude_tags
        )
        additional_filters = self._filters or []

        terms_agg = agg.Terms(
            self.field, instance_mapper=self._instance_mapper,
            **self._agg_kwargs
        )
        if filters or additional_filters:
            aggs = {
                self._filter_agg_name: agg.Filter(
                    Bool.must(*(filters + additional_filters)),
                    aggs={self._agg_name: terms_agg}
                )
            }
        else:
            aggs = {self._agg_name: terms_agg}
        return search_query.aggregations(**aggs)

    def _process_result(self, result, params):
        values = self._get_values_from_params(params.get(self.alias, {}))

        if result.get_aggregation(self._filter_agg_name):
            terms_agg = result \
                .get_aggregation(self._filter_agg_name) \
                .get_aggregation(self._agg_name)
        else:
            terms_agg = result.get_aggregation(self._agg_name)

        facet_result = FacetFilterResult(self.name, self.alias)
        processed_values = set()
        for bucket in terms_agg.buckets:
            # FIXME: values can be a list of string but bucket.key may not
            facet_result.add_value(FacetValueResult(
                bucket, bucket.key in values, bool(values),
                get_title=self._get_title,
            ))
            if bucket.key in values:
                self.qf._set_selected(self.name, bucket.key)
            self.qf._set_value_data(self.name, bucket.key, {'bucket': bucket})
            self.add_value(FacetValue(bucket.key, _filter=self))
            processed_values.add(bucket.key)

        for v in values:
            if v not in processed_values:
                fake_agg_data = {'key': v, 'doc_count': None}
                fake_bucket = terms_agg.bucket_cls(
                    fake_agg_data, terms_agg.expr.aggs(None), terms_agg
                )
                terms_agg.add_bucket(fake_bucket)
                self.qf._set_selected(self.name, fake_bucket.key)
                self.qf._set_value_data(
                    self.name, fake_bucket.key, {'bucket': fake_bucket}
                )
                self.add_value(FacetValue(fake_bucket.key).bind(self))
                facet_result.add_value(FacetValueResult(
                    fake_bucket, True, True, get_title=self._get_title,
                ))

        return facet_result

    def add_value(self, fv):
        self.all_values.append(fv)
        self.values_map[fv.value] = fv
        if fv.selected:
            self.selected_values.append(fv)
        else:
            self.values.append(fv)

    def get_value(self, value):
        return self.values_map.get(value)


class FacetFilterResult(BaseFilterResult):
    def __init__(self, name, alias):
        super(FacetFilterResult, self).__init__(name, alias)
        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    def add_value(self, fv):
        self.all_values.append(fv)
        self.values_map[fv.value] = fv
        if fv.selected:
            self.selected_values.append(fv)
        else:
            self.values.append(fv)

    def get_value(self, value):
        return self.values_map.get(value)


class FacetValueResult(object):
    def __init__(self, bucket, selected,
                 filter_has_selected_values, get_title=None):
        self.bucket = bucket
        self.selected = selected
        self._filter_has_selected_values = filter_has_selected_values
        self._get_title = get_title

    @property
    def value(self):
        return self.bucket.key

    @property
    def count(self):
        return self.bucket.doc_count

    @property
    def count_text(self):
        if self.count is None:
            return ''
        if not self.selected and self._filter_has_selected_values:
            return '+{}'.format(self.count)
        return '{}'.format(self.count)

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
    def title(self):
        if self._get_title:
            return self._get_title(self)
        if self.instance:
            return text_type(self.instance)
        return text_type(self.value)

    def __str__(self):
        return text_type(self.title)

    __unicode__ = __str__


class BinaryFilter(BaseFilter):
    def __init__(self, name, condition, p_key=None, p_value=None, alias=None):
        super(BinaryFilter, self).__init__(name, alias=alias)
        self._condition = condition
        self.p_key = p_key
        self.p_value = p_value
        self._reset()

    def _parameters_condition(self, params):
        if self.p_key is None:
            return params.get(
                self.name, {'exact': [None]}
            ).get('exact', [None])[0] == u"true"
        else:
            return params.get(
                self.p_key, {'exact': [None]}
            ).get('exact', [None])[0] == self.p_value

    def _reset(self):
        self.selected = False
        self.count = 0

    def _apply_filter(self, search_query, params):
        self.selected = self._parameters_condition(params)
        if not self.selected:
            return search_query
        return search_query.filter(self._condition)

    @property
    def _agg_name(self):
        return '{}.{}'.format(self.qf._name, self.name)

    def _apply_agg(self, search_query):
        if self.selected:
            return search_query
        agg_filters = search_query.get_context().post_filters
        return search_query.aggregations(
            **{
                self._agg_name: agg.Filter(
                    Bool.must(*chain(agg_filters, [self._condition])))
            })

    def _process_result(self, result, params):
        if not self.selected:
            aggregation = result.get_aggregation(self._agg_name)
            if aggregation:
                self.count = aggregation.doc_count


# TODO Remove this
class Facet(FacetFilter):
    """
    Non-filtering version of facet filter
    """

    def _apply_filter(self, search_query, params):
        return search_query


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
    def count_text(self):
        if self.count is None:
            return ''
        if not self.selected and self.filter.selected_values:
            return '+{}'.format(self.count)
        return '{}'.format(self.count)

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
    def title(self):
        if self.filter._get_title:
            return self.filter._get_title(self)
        if self.instance:
            return text_type(self.instance)
        return text_type(self.value)

    def __str__(self):
        return text_type(self.title)

    __unicode__ = __str__


class RangeFilter(FieldFilter):

    def __init__(
            self, name, field, alias=None, type=None,
            compute_enabled=True, compute_min_max=True
    ):
        super(RangeFilter, self).__init__(name, field, alias=alias)
        self.type = instantiate(type or self.field.get_type())
        self._compute_enabled = compute_enabled
        self._compute_min_max = compute_min_max
        self.from_value = None
        self.to_value = None
        self.enabled = None
        self.min = None
        self.max = None

    def _reset(self):
        self.from_value = None
        self.to_value = None
        self.enabled = None
        self.min = None
        self.max = None

    @property
    def _filter_agg_name(self):
        return '{}.{}.filter'.format(self.qf._name, self.name)

    @property
    def _min_agg_name(self):
        return '{}.{}.min'.format(self.qf._name, self.name)

    @property
    def _max_agg_name(self):
        return '{}.{}.max'.format(self.qf._name, self.name)

    @property
    def _enabled_agg_name(self):
        return '{}.{}.enabled'.format(self.qf._name, self.name)

    def _get_from_value(self, params):
        from_values = params.get('gte')
        if from_values:
            return from_values[0]
        from_values = params.get('exact')
        if from_values:
            return from_values[0]

    def _get_to_value(self, params):
        to_values = params.get('lte')
        if to_values:
            return to_values[0]
        to_values = params.get('exact')
        if to_values:
            return to_values[-1]

    def _apply_filter(self, search_query, params):
        params = params.get(self.alias) or {}
        self.from_value = self._get_from_value(params)
        self.to_value = self._get_to_value(params)
        if self.from_value is None and self.to_value is None:
            return search_query

        return search_query.post_filter(
            self.field.range(gte=self.from_value, lte=self.to_value),
            meta={'tags': {self.name}}
        )

    def _apply_agg(self, search_query):
        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            {self.qf._name, self.name}
        )

        aggs = {}
        if self._compute_enabled:
            aggs.update({
                self._enabled_agg_name: agg.Filter(self.field != None),  # noqa:E711
            })

        if self._compute_min_max:
            stat_aggs = {
                self._min_agg_name: agg.Min(self.field),
                self._max_agg_name: agg.Max(self.field),
            }
            if filters:
                aggs.update({
                    self._filter_agg_name: agg.Filter(
                        Bool.must(*filters), aggs=stat_aggs
                    )
                })
            else:
                aggs.update(stat_aggs)

        return search_query.aggregations(**aggs)

    def _process_result(self, result, params):
        if result.get_aggregation(self._filter_agg_name):
            base_agg = result.get_aggregation(self._filter_agg_name)
        else:
            base_agg = result

        if self._compute_enabled:
            self.enabled = bool(
                result.get_aggregation(self._enabled_agg_name).doc_count
            )
        if self._compute_min_max:
            self.min = base_agg.get_aggregation(self._min_agg_name).value
            self.max = base_agg.get_aggregation(self._max_agg_name).value
        return RangeFilterResult(
            self.from_value, self.to_value,
            enabled=self.enabled, min_value=self.min, max_value=self.max
        )


class RangeFilterResult(object):
    def __init__(
            self, from_value, to_value,
            enabled=None, min_value=None, max_value=None
    ):
        self.from_value = from_value
        self.to_value = to_value
        self.enabled = enabled
        self.min_value = min_value
        self.max_value = max_value

    @property
    def min(self):
        return self.min_value

    @property
    def max(self):
        return self.max_value


class SimpleQueryValue(BaseFilterValue):
    def __init__(self, value, expr, _filter=None, **opts):
        super(SimpleQueryValue, self).__init__(value, _filter=_filter)
        self.expr = expr
        self.opts = opts

    def bind(self, filter):
        return self.__class__(
            self.value, self.expr, _filter=filter, **self.opts
        )


class SimpleQueryFilter(BaseFilter):
    def __init__(self, name, *values, **kwargs):
        super(SimpleQueryFilter, self).__init__(
            name, alias=kwargs.pop('alias', None)
        )
        self._values = [fv.bind(self) for fv in values]
        self._values_map = {fv.value: fv for fv in self._values}
        self._conj_operator = kwargs.pop('conj_operator', QueryFilter.CONJ_OR)
        self.default = kwargs.pop('default', None)

    @property
    def all_values(self):
        return self._values

    @property
    def _types(self):
        return {self.alias: None}

    def get_value(self, value):
        return self._values_map.get(value)

    def _get_expression(self, params):
        values = params.get(self.alias, {}).get('exact')
        if not values:
            if self.default:
                values = [self.default]
        if not values:
            return None

        expressions = []
        for v in values:
            filter_value = self.get_value(v)
            if filter_value and not isinstance(filter_value.expr, MatchAll):
                expressions.append(filter_value.expr)

        if not expressions:
            return None

        if self._conj_operator == QueryFilter.CONJ_AND:
            return Bool.must(*expressions)
        else:
            return Bool.should(*expressions)

    def _apply_filter(self, search_query, params):
        expr = self._get_expression(params)
        if expr is None:
            return search_query
        return search_query.filter(expr, meta={'tags': {self.name}})


class FacetQueryValue(BaseFilterValue):
    def __init__(self, value, expr, _filter=None, **opts):
        super(FacetQueryValue, self).__init__(value, _filter=_filter)
        self.expr = expr
        self.opts = opts

    def bind(self, filter):
        return self.__class__(
            self.value, self.expr, _filter=filter, **self.opts
        )

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
        if self.count is None:
            return ''
        if not self.selected and self.filter.selected_values:
            return '+{}'.format(self.count)
        return '{}'.format(self.count)

    @property
    def filter_name(self):
        return self.filter.name

    @property
    def filter_value(self):
        return self.value

    @property
    def is_default(self):
        return self.value == self.filter.default

    @property
    def title(self):
        return text_type(self.opts.get('title', self.value))

    def __str__(self):
        return text_type(self.title)

    __unicode__ = __str__


class FacetQueryFilter(SimpleQueryFilter):
    def __init__(self, name, *values, **kwargs):
        super(FacetQueryFilter, self).__init__(name, *values, **kwargs)
        self.agg_kwargs = kwargs

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
        return self._values_map.get(value)

    @property
    def _filter_agg_name(self):
        return '{}.{}.filter'.format(self.qf._name, self.name)

    def _make_agg_name(self, value):
        return '{}.{}:{}'.format(self.qf._name, self.name, value)

    def _apply_filter(self, search_query, params):
        expr = self._get_expression(params)
        if expr is None:
            return search_query
        return search_query.post_filter(expr, meta={'tags': {self.name}})

    def _apply_agg(self, search_query):
        exclude_tags = {self.qf._name}
        if self._conj_operator == QueryFilter.CONJ_OR:
            exclude_tags.add(self.name)
        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            exclude_tags
        )

        filter_aggs = {}
        for fv in self.values:
            filter_aggs[self._make_agg_name(fv.value)] = agg.Filter(
                fv.expr, **self.agg_kwargs
            )

        if filters:
            aggs = {
                self._filter_agg_name: agg.Filter(
                    Bool.must(*filters), aggs=filter_aggs
                )
            }
        else:
            aggs = filter_aggs

        return search_query.aggregations(**aggs)

    def _process_result(self, result, params):
        values = params.get(self.alias, {}).get('exact', [])
        if result.get_aggregation(self._filter_agg_name):
            filters_agg = result.get_aggregation(self._filter_agg_name)
        else:
            filters_agg = result
        for fv in self.values:
            filt_agg = filters_agg.get_aggregation(
                self._make_agg_name(fv.value)
            )
            if fv.value in values:
                self.qf._set_selected(self.name, fv.value)
            self.qf._set_value_data(self.name, fv.value, {'agg': filt_agg})
        facet_result = FacetQueryFilterResult()
        has_selected_values = any(
            map(lambda fv: fv.value in values, self._values)
        )
        for fv in self._values:
            filt_agg = filters_agg.get_aggregation(
                self._make_agg_name(fv.value)
            )
            facet_result.add_value(FacetQueryValueResult(
                fv.value, filt_agg, fv.value in values, has_selected_values
            ))
        return facet_result


class FacetQueryFilterResult(object):
    def __init__(self):
        self._values = []
        self._values_map = {}

    def add_value(self, fv):
        self._values.append(fv)
        self._values_map[fv.value] = fv

    def get_value(self, value):
        return self._values_map.get(value)

    @property
    def all_values(self):
        return self._values

    @property
    def selected_values(self):
        return [fv for fv in self._values if fv.selected]

    @property
    def values(self):
        return [fv for fv in self._values if not fv.selected]


class FacetQueryValueResult(object):
    def __init__(self, value, agg, selected, filter_has_selected_values):
        self.value = value
        self.agg = agg
        self.selected = selected
        self._filter_has_selected_values = filter_has_selected_values

    @property
    def count(self):
        agg = self.agg
        if agg:
            return self.agg.doc_count

    @property
    def count_text(self):
        if self.count is None:
            return ''
        if not self.selected and self._filter_has_selected_values:
            return '+{}'.format(self.count)
        return '{}'.format(self.count)

    @property
    def filter_name(self):
        return self.filter.name

    @property
    def filter_value(self):
        return self.value

    @property
    def is_default(self):
        return self.value == self.filter.default

    @property
    def title(self):
        return text_type(self.opts.get('title', self.value))

    def __str__(self):
        return text_type(self.title)

    __unicode__ = __str__


class OrderingValue(BaseFilterValue):
    def __init__(self, value, orderings, _filter=None, **kwargs):
        super(OrderingValue, self).__init__(value, _filter=_filter)
        self.orderings = orderings
        self.opts = kwargs

    def bind(self, filter):
        return self.__class__(
            self.value, self.orderings, _filter=filter, **self.opts
        )

    def __str__(self):
        return text_type(self.opts.get('title', self.value))

    __unicode__ = __str__

    def _apply(self, query):
        return query.order_by(*self.orderings)


class OrderingFilter(BaseFilter):
    def __init__(self, name, *values, **kwargs):
        super(OrderingFilter, self).__init__(
            name, alias=kwargs.pop('alias', None)
        )
        self.values = [fv.bind(self) for fv in values]
        self.default_value = (
            self.get_value(kwargs.get('default')) or self.values[0]
        )
        self._values_map = {fv.value: fv for fv in self.values}
        self.selected_value = None

    def get_value(self, value):
        for ordering_value in self.values:
            if ordering_value.value == value:
                return ordering_value

    def _reset(self):
        self.selected_value = None

    def _apply_filter(self, search_query, params):
        values = params.get(self.alias, {}).get('exact')

        ordering_value = None
        if values:
            ordering_value = self.get_value(values[0])
        if not ordering_value:
            ordering_value = self.default_value

        self.selected_value = ordering_value
        self.qf._set_selected(self.name, ordering_value.value)
        return ordering_value._apply(search_query)

    def _get_selected_value(self, values):
        if values and values[0] in self._values_map:
            return self._values_map[values[0]]
        return self.default_value

    def _process_result(self, result, params):
        values = params.get(self.alias, {}).get('exact')
        selected_fv = self._get_selected_value(values)

        ordering_result = OrderingFilterResult()

        for fv in self.values:
            ordering_result.add_value(
                OrderingValueResult(fv.value,
                                    fv is selected_fv,
                                    fv is self.default_value)
            )
        return ordering_result


class OrderingFilterResult(object):
    def __init__(self):
        self.values = []
        self._values_map = {}
        self.default_value = None
        self.selected_value = None

    def add_value(self, value):
        self.values.append(value)
        self._values_map[value.value] = value
        if value.selected:
            self.selected_value = value
        if value.is_default:
            self.default_value = value

    def get_value(self, value):
        return self._values_map.get(value)


class OrderingValueResult(object):
    def __init__(self, value, selected, is_default, title=None):
        self.value = value
        self.selected = selected
        self.is_default = is_default
        self.title = title

    def __str__(self):
        return text_type(self.title)

    __unicode__ = __str__


class PageFilter(BaseFilter):
    DEFAULT_PER_PAGE_PARAM = 'per_page'
    DEFAULT_PER_PAGE = 10
    DEFAULT_MAX_ITEMS = MAX_RESULT_WINDOW

    def __init__(
            self, name, alias=None,
            per_page_param=None, per_page_values=None, max_items=None,
    ):
        super(PageFilter, self).__init__(name, alias=alias)
        self.per_page_param = per_page_param or self.DEFAULT_PER_PAGE_PARAM
        self.per_page_values = per_page_values or [self.DEFAULT_PER_PAGE]
        self.max_items = max_items or self.DEFAULT_MAX_ITEMS

        self._reset()

    def _reset(self):
        self.per_page = None
        self.page = None
        self.offset = None
        self.limit = None
        self.total = None
        self.items = None
        self.pages = None

    @property
    def _types(self):
        return {
            self.alias: Integer,
            self.per_page_param: Integer,
        }

    def _get_per_page(self, params):
        per_page_from_param = params.get(self.per_page_param, {}).get('exact')
        if per_page_from_param:
            per_page = per_page_from_param[0]
        else:
            per_page = self.per_page_values[0]
        if per_page not in self.per_page_values:
            per_page = self.per_page_values[0]
        return per_page

    def _get_page(self, params):
        page_num = params.get(self.alias, {}).get('exact')
        if page_num and page_num[0] is not None:
            return page_num[0]
        return 1

    def _apply_filter(self, search_query, params):
        self.per_page = self._get_per_page(params)
        self.page = self._get_page(params)

        search_query = search_query.limit(self.per_page)

        self.offset = (self.page - 1) * self.per_page
        self.limit = self.per_page
        if self.max_items and self.offset + self.limit > self.max_items:
            self.limit = max(self.max_items - self.offset, 0)

        search_query = search_query.limit(self.limit)
        if self.offset > 0 and self.limit > 0:
            search_query = search_query.offset(self.offset)

        return search_query

    def _process_result(self, result, params):
        self.total = result.total
        self.items = result.hits
        self.pages = int(ceil(self.total / float(self.per_page)))
        self.has_prev = self.page > 1
        self.has_next = self.page < self.pages
        return PageFilterResult(
            result.total, result.hits,
            per_page_param=self.per_page_param,
            per_page_values=self.per_page_values,
            max_items=self.max_items,
            page=self.page,
            per_page=self.per_page,
            offset=self.offset,
            limit=self.limit,
        )


class PageFilterResult(object):
    def __init__(
            self, total, hits,
            per_page_param, per_page_values, max_items,
            page, per_page, offset=None, limit=None
    ):
        self.total = total
        self.items = hits
        self.per_page_param = per_page_param
        self.per_page_values = per_page_values
        self.max_items = max_items
        self.page = page
        self.per_page = per_page
        self.offset = offset
        self.limit = limit
        self.pages = int(ceil(total / float(per_page)))
        self.has_prev = self.page > 1
        self.has_next = self.page < self.pages


class NestedFacetFilter(BaseFilter):

    def __init__(
        self, name, path, key_expression, value_field,
        alias=None, type=None, conj_operator=QueryFilter.CONJ_OR,
        instance_mapper=None, get_title=None, **kwargs
    ):
        super(NestedFacetFilter, self).__init__(name, alias=alias)
        self._instance_mapper = instance_mapper
        self._get_title = get_title
        self._agg_kwargs = kwargs
        self._conj_operator = conj_operator

        self.type = instantiate(type or value_field.get_type())
        self.key_expression = key_expression
        self.value_field = value_field
        self.path = path

        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    def _reset(self):
        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    @property
    def _types(self):
        return {self.alias: self.type}

    @staticmethod
    def _get_values_from_params(params):
        values = params.get('exact', [])
        return list(filter(is_not_none, values))

    def _get_expression(self, params):
        values = self._get_values_from_params(params.get(self.alias, {}))
        if not values:
            return None

        if len(values) == 1:
            return Nested(
                path=self.path,
                query=Bool.must(
                    self.key_expression,
                    self.value_field == values[0]
                )
            )

        expressions = [self.key_expression]
        if self._conj_operator == QueryFilter.CONJ_AND:
            expressions.extend([self.value_field == v for v in values])
        else:
            expressions.append(self.value_field.in_(values))

        return Nested(
            path=self.path,
            query=Bool.must(*expressions)
        )

    def _apply_filter(self, search_query, params):
        expr = self._get_expression(params)
        if expr is None:
            return search_query
        return search_query.post_filter(expr, meta={'tags': {self.name}})

    @property
    def _agg_name(self):
        return '{}.{}'.format(self.qf._name, self.name)

    @property
    def _filter_agg_name(self):
        return '{}.{}.filter'.format(self.qf._name, self.name)

    @property
    def _filter_key_agg_name(self):
        return '{}.{}.key'.format(self.qf._name, self.name)

    @property
    def _filter_value_agg_name(self):
        return '{}.{}.value'.format(self.qf._name, self.name)

    def _apply_agg(self, search_query):
        exclude_tags = {self.qf._name}
        if self._conj_operator == QueryFilter.CONJ_OR:
            exclude_tags.add(self.name)

        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            exclude_tags
        )

        terms_agg = agg.Nested(path=self.path, aggs={
            self._filter_key_agg_name: agg.Filter(
                self.key_expression,
                aggs={
                    self._filter_value_agg_name: agg.Terms(
                        self.value_field,
                        instance_mapper=self._instance_mapper,
                        **self._agg_kwargs
                    )
                },
                **self._agg_kwargs
            )
        })
        if filters:
            aggs = {
                self._filter_agg_name: agg.Filter(
                    Bool.must(*filters), aggs={self._agg_name: terms_agg}
                )
            }
        else:
            aggs = {self._agg_name: terms_agg}

        return search_query.aggregations(**aggs)

    def _process_result(self, result, params):
        values = self._get_values_from_params(params.get(self.alias, {}))

        if result.get_aggregation(self._filter_agg_name):
            terms_agg = (
                result
                .get_aggregation(self._filter_agg_name)
                .get_aggregation(self._agg_name)
                .get_aggregation(self._filter_key_agg_name)
                .get_aggregation(self._filter_value_agg_name)
            )
        else:
            terms_agg = (
                result
                .get_aggregation(self._agg_name)
                .get_aggregation(self._filter_key_agg_name)
                .get_aggregation(self._filter_value_agg_name)
            )

        facet_result = FacetFilterResult(self.name, self.alias)
        processed_values = set()
        for bucket in terms_agg.buckets:
            if bucket.key in values:
                self.qf._set_selected(self.name, bucket.key)
            self.qf._set_value_data(self.name, bucket.key, {'bucket': bucket})
            self.add_value(FacetValue(bucket.key, _filter=self))
            facet_result.add_value(FacetValueResult(
                bucket, bucket.key in values, bool(values),
                get_title=self._get_title,
            ))
            processed_values.add(bucket.key)

        for v in values:
            if v not in processed_values:
                fake_agg_data = {'key': v, 'doc_count': None}
                fake_bucket = terms_agg.bucket_cls(
                    fake_agg_data, terms_agg.expr.aggs(None), terms_agg
                )
                terms_agg.add_bucket(fake_bucket)
                self.qf._set_selected(self.name, fake_bucket.key)
                self.qf._set_value_data(
                    self.name, fake_bucket.key, {'bucket': fake_bucket}
                )
                self.add_value(FacetValue(fake_bucket.key).bind(self))
                facet_result.add_value(FacetValueResult(
                    fake_bucket, True, True, get_title=self._get_title,
                ))

        return facet_result

    def add_value(self, fv):
        self.all_values.append(fv)
        self.values_map[fv.value] = fv
        if fv.selected:
            self.selected_values.append(fv)
        else:
            self.values.append(fv)

    def get_value(self, value):
        return self.values_map.get(value)


class NestedRangeFilter(BaseFilter):

    def __init__(
            self, name, path, key_expression, value_field, alias=None,
            type=None, compute_enabled=True, compute_min_max=None,
    ):
        super(NestedRangeFilter, self).__init__(name, alias=alias)

        self.type = instantiate(type or value_field.get_type())
        self.key_expression = key_expression
        self.value_field = value_field
        self.path = path

        self._compute_enabled = compute_enabled
        self._compute_min_max = compute_min_max
        self.from_value = None
        self.to_value = None
        self.enabled = None
        self.min = None
        self.max = None

    def _reset(self):
        self.from_value = None
        self.to_value = None
        self.enabled = None
        self.min = None
        self.max = None

    @property
    def _types(self):
        return {self.alias: self.type}

    @property
    def _filter_agg_name(self):
        return '{}.{}.filter'.format(self.qf._name, self.name)

    @property
    def _filter_key_agg_name(self):
        return '{}.{}.key'.format(self.qf._name, self.name)

    @property
    def _filter_value_agg_name(self):
        return '{}.{}.value'.format(self.qf._name, self.name)

    @property
    def _min_agg_name(self):
        return '{}.{}.min'.format(self.qf._name, self.name)

    @property
    def _max_agg_name(self):
        return '{}.{}.max'.format(self.qf._name, self.name)

    @property
    def _enabled_agg_name(self):
        return '{}.{}.enabled'.format(self.qf._name, self.name)

    @property
    def _enabled_agg_name_stat(self):
        return '{}.{}.enabled.stat'.format(self.qf._name, self.name)

    @staticmethod
    def _get_from_value(params):
        from_values = params.get('gte')
        return from_values[0] if from_values else None

    @staticmethod
    def _get_to_value(params):
        to_values = params.get('lte')
        return to_values[0] if to_values else None

    def _apply_filter(self, search_query, params):
        params = params.get(self.alias) or {}
        self.from_value = self._get_from_value(params)
        self.to_value = self._get_to_value(params)
        if self.from_value is None and self.to_value is None:
            return search_query

        expr = Nested(
            path=self.path,
            query=Bool.must(
                self.key_expression,
                self.value_field.range(gte=self.from_value, lte=self.to_value),
            )
        )
        return search_query.post_filter(expr, meta={'tags': {self.name}})

    def _apply_agg(self, search_query):
        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            {self.qf._name, self.name}
        )

        aggs = {}
        if self._compute_enabled:
            aggs.update({
                self._enabled_agg_name: agg.Nested(
                    path=self.path,
                    aggs={
                        self._filter_key_agg_name: agg.Filter(
                            self.key_expression,
                            aggs={
                                self._filter_value_agg_name: agg.Filter(
                                    self.value_field != None  # noqa:E711
                                )
                            }
                        )
                    }
                )
            })
        if self._compute_min_max:
            stat_aggs = {
                self._enabled_agg_name_stat: agg.Nested(
                    path=self.path,
                    aggs={
                        self._filter_key_agg_name: agg.Filter(
                            self.key_expression,
                            aggs={
                                self._min_agg_name: agg.Min(self.value_field),
                                self._max_agg_name: agg.Max(self.value_field),
                            }
                        )
                    }
                )
            }
            if filters:
                aggs.update({
                    self._filter_agg_name: agg.Filter(
                        Bool.must(*filters), aggs=stat_aggs
                    )
                })
            else:
                aggs.update(stat_aggs)

        return search_query.aggregations(**aggs)

    def _process_result(self, result, params):
        if result.get_aggregation(self._filter_agg_name):
            base_agg = result.get_aggregation(self._filter_agg_name)
        else:
            base_agg = result

        if self._compute_enabled:
            self.enabled = bool(
                result
                .get_aggregation(self._enabled_agg_name)
                .get_aggregation(self._filter_key_agg_name)
                .get_aggregation(self._filter_value_agg_name)
                .doc_count
            )
        if self._compute_min_max:
            base_agg = (
                base_agg
                .get_aggregation(self._enabled_agg_name_stat)
                .get_aggregation(self._filter_key_agg_name)
            )
            self.min = base_agg.get_aggregation(self._min_agg_name).value
            self.max = base_agg.get_aggregation(self._max_agg_name).value
        return RangeFilterResult(
            self.from_value, self.to_value,
            enabled=self.enabled,
            min_value=self.min,
            max_value=self.max,
        )
