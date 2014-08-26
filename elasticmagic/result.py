from .document import Document


class Result(object):
    def __init__(self, raw_result, aggregations, doc_cls=None):
        self.raw = raw_result
        self._query_aggs = aggregations
        self.doc_cls = doc_cls or Document

        self.total = raw_result['hits']['total']
        self.hits = []
        for hit in raw_result['hits']['hits']:
            self.hits.append(self.doc_cls(_hit=hit))

        self.aggregations = {}
        for agg_name, agg_data in raw_result.get('aggregations', {}).items():
            agg_instance = self.aggregations[agg_name] = self._query_aggs[agg_name].clone()
            agg_instance.process_results(agg_data)

    def __iter__(self):
        return iter(self.hits)

    def get_aggregation(self, name):
        return self.aggregations.get(name)
