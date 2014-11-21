from .agg import BucketAgg
from .document import Document


class Result(object):
    def __init__(self, raw_result, aggregations=None,
                 doc_cls=None, instance_mapper=None):
        self.raw = raw_result
        self._query_aggs = aggregations or {}
        if not doc_cls:
            self._doc_classes = (Document,)
        elif isinstance(doc_cls, tuple):
            self._doc_classes = doc_cls
        else:
            self._doc_classes = (doc_cls,)
        self._doc_cls_map = {doc_cls.__doc_type__: doc_cls for doc_cls in self._doc_classes}
        if isinstance(instance_mapper, dict):
            self._instance_mappers = instance_mapper
        else:
            self._instance_mappers = {doc_cls: instance_mapper for doc_cls in self._doc_classes}

        self.total = raw_result['hits']['total']
        self.hits = []
        for hit in raw_result['hits']['hits']:
            doc_cls = self._doc_cls_map[hit['_type']]
            self.hits.append(doc_cls(_hit=hit, _result=self))

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

    def _populate_instances(self, doc_cls):
        docs = [doc for doc in self.hits if isinstance(doc, doc_cls)]
        instances = self._instance_mappers[doc_cls]([doc._id for doc in docs])
        for doc in docs:
            doc.__dict__['instance'] = instances.get(doc._id)
