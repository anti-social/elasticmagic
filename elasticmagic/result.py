import collections

from .agg import BucketAgg
from .document import DynamicDocument


class Result(object):
    def __init__(self, raw_result, aggregations=None,
                 doc_cls=None, instance_mapper=None):
        self.raw = raw_result
        self._query_aggs = aggregations or {}

        if doc_cls is None:
            doc_classes = ()
        elif not isinstance(doc_cls, collections.Iterable):
            doc_classes = (doc_cls,)
        else:
            doc_classes = doc_cls
        self._doc_cls_map = {doc_cls.__doc_type__: doc_cls for doc_cls in doc_classes}

        if isinstance(instance_mapper, dict):
            self._instance_mappers = instance_mapper
        else:
            self._instance_mappers = {doc_cls: instance_mapper for doc_cls in doc_classes}

        self.total = raw_result['hits']['total']
        self.hits = []
        for hit in raw_result['hits']['hits']:
            doc_cls = self._doc_cls_map.get(hit['_type'], DynamicDocument)
            self.hits.append(doc_cls(_hit=hit, _result=self))

        self.aggregations = {}
        self._mapper_registry = {}
        for agg_name, agg_expr in self._query_aggs.items():
            raw_agg_data = raw_result['aggregations'][agg_name]
            agg_result = agg_expr.build_agg_result(raw_agg_data, mapper_registry=self._mapper_registry)
            self.aggregations[agg_name] = agg_result

        self.scroll_id = raw_result.get('_scroll_id')
            
    def __iter__(self):
        return iter(self.hits)

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def _populate_instances(self, doc_cls):
        docs = [doc for doc in self.hits if isinstance(doc, doc_cls)]
        instances = self._instance_mappers.get(doc_cls)([doc._id for doc in docs])
        for doc in docs:
            doc.__dict__['instance'] = instances.get(doc._id)


class ActionResult(object):
    def __init__(self, raw):
        self.raw = raw
        self.name = next(iter(raw.keys()))
        data = next(iter(raw.values()))
        self.status = data['status']
        self.found = data.get('found')
        self.error = data.get('error')
        self._index = data['_index']
        self._type = data['_type']
        self._id = data['_id']
        self._version = data.get('_version')


class BulkResult(object):
    def __init__(self, raw):
        self.raw = raw
        self.took = raw['took']
        self.errors = raw['errors']
        self.items = list(map(ActionResult, raw['items']))

    def __iter__(self):
        return iter(self.items)
