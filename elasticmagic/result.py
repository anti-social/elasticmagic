from .agg import BucketAgg
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
        self._mapper_registry = {}
        for agg_name, agg_expr in self._query_aggs.items():
            raw_agg_data = raw_result['aggregations'][agg_name]
            agg_result = agg_expr.build_agg_result(raw_agg_data, mapper_registry=self._mapper_registry)
            self.aggregations[agg_name] = agg_result

    def __iter__(self):
        return iter(self.hits)

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def _populate_instances(self):
        ids = [doc._id for doc in self.hits]
        instances = self.instance_mapper(ids)
        for doc in self.hits:
            doc.__dict__['instance'] = instances.get(doc._id)
