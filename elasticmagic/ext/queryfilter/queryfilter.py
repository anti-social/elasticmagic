from elasticmagic import Term, Terms, Query, And, agg, types


class QueryFilter(object):
    DEFAULT_NAME = 'qf'

    def __init__(self, name=None):
        self.name = name or self.DEFAULT_NAME
        self.filters = []
        self.params = {}

    def add_filter(self, filter):
        self.filters.append(filter)

    def apply(self, search_query, params):
        self.params = params

        # First filter query
        for f in self.filters:
            search_query = f._apply_filter(search_query, params.get(f.name))

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
            f._process_agg(main_agg, self.params.get(f.name))

    def get_filter(self, name):
        for f in self.filters:
            if f.name == name:
                return f


class Facet(object):
    def __init__(self, name, field, alias=None, type=None, instance_mapper=None):
        self.name = name
        self.field = field
        self.alias = alias or self.name
        self.type = types.instantiate(type)
        self.instance_mapper = instance_mapper

        self.values = []
        self.selected_values = []
        self.all_values = []
        self.values_map = {}

    def _apply_filter(self, search_query, params):
        if not params:
            return search_query

        if self.type:
            params = [self.type.to_python(p) for p in params]

        if len(params) == 1:
            filt = Term(self.field, params[0])
        else:
            filt = Terms(self.field, params)

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
        params = params or []
        if self.type:
            params = [self.type.to_python(p) for p in params]

        terms_agg = main_agg.get_aggregation(self.name).get_aggregation(self.name)
        for bucket in terms_agg.buckets:
            selected = bucket.key in params
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
