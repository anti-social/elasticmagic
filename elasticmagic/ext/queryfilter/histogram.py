from ... import agg
from .queryfilter import BaseFilterResult
from .queryfilter import QueryFilter
from .queryfilter import SimpleFilter


class HistogramQueryFilter(SimpleFilter):
    def __init__(
            self, name, field, interval, min_doc_count=None, alias=None,
            type=None, conj_operator=QueryFilter.CONJ_OR, **kwargs
    ):
        super(HistogramQueryFilter, self).__init__(
            name, field, alias=alias, type=type, conj_operator=conj_operator
        )
        self._allow_null = False
        self._agg_kwargs = kwargs

        self.interval = interval
        self.min_doc_count = min_doc_count

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

    def _apply_agg(self, search_query, params):
        exclude_tags = {self.qf._name}
        if self._conj_operator == QueryFilter.CONJ_OR:
            exclude_tags.add(self.name)
        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            exclude_tags
        )

        histogram_agg = agg.Histogram(
            self.field,
            interval=self.interval,
            min_doc_count=self.min_doc_count,
            **self._agg_kwargs)

        if filters:
            aggs = {
                self._agg_name: agg.Filter(
                    Bool.must(*filters), aggs={"histogram": histogram_agg}
                )
            }
            self.filtered = True
        else:
            aggs = {self._agg_name: histogram_agg}
            self.filtered = False
        return search_query.aggregations(**aggs)

    def _process_result(self, result, params):
        histogram_agg = result.get_aggregation(self._agg_name)
        if self.filtered:
            filtered_agg = histogram_agg.get_aggregation("histogram")
            if filtered_agg:
                histogram_agg = filtered_agg
        return HistogramFilterResult(
            self.name, self.alias, self.interval, self.min_doc_count,
            histogram_agg.buckets
        )


class HistogramFilterResult(BaseFilterResult):
    def __init__(self, name, alias, interval, min_doc_count, buckets):
        super(HistogramFilterResult, self).__init__(name, alias)
        self.interval = interval
        self.min_doc_count = min_doc_count
        self.columns = buckets
    #     self.selected_values = []
    #     self.all_values = []
    #     self.values_map = {}

    # def add_value(self, fv):
    #     self.all_values.append(fv)
    #     self.values_map[fv.value] = fv
    #     if fv.selected:
    #         self.selected_values.append(fv)
    #     else:
    #         self.values.append(fv)

    # def get_value(self, value):
    #     return self.values_map.get(value)
