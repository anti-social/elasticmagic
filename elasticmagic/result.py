from .document import Document


def _nop_instance_mapper(ids):
    return {}


class Result(object):
    def __init__(self, raw_result, aggregations=None,
                 doc_cls=None, instance_mapper=None):
        self.raw = raw_result
        self._query_aggs = aggregations or {}
        self.doc_cls = doc_cls or Document
        self.instance_mapper = instance_mapper or _nop_instance_mapper

        self.total = raw_result['hits']['total']
        self.hits = []
        for hit in raw_result['hits']['hits']:
            self.hits.append(self.doc_cls(_hit=hit, _result=self))

        self.aggregations = {}
        for agg_name, agg_data in raw_result.get('aggregations', {}).items():
            if agg_name in self._query_aggs:
                agg_instance = self.aggregations[agg_name] = self._query_aggs[agg_name].clone()
                agg_instance.process_results(agg_data)

    def __iter__(self):
        return iter(self.hits)

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def _populate_instances(self):
        ids = [doc._id for doc in self.hits]
        instances = self.instance_mapper(ids)
        for doc in self.hits:
            doc.__dict__['instance'] = instances.get(doc._id)
