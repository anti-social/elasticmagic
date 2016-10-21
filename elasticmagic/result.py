import collections

from elasticsearch import ElasticsearchException

from .agg import BucketAgg
from .compat import string_types
from .document import DynamicDocument
from .util import to_camel_case


class DelayedElasticsearchException(ElasticsearchException):
    pass


class Result(object):
    def __init__(self, raw_result):
        self.raw = raw_result


class DocProcessor(object):
    def __init__(self, doc_cls_map, raw_result):
        self._doc_cls_map = doc_cls_map
        self._raw_result = raw_result

    def process(self, hit, result):
        raise NotImplementedError()


class DefaultDocProcessor(DocProcessor):
    def process(self, hit, result):
        doc_cls = self._doc_cls_map.get(hit['_type'], DynamicDocument)
        return doc_cls(_hit=hit, _result=result)


class NamedtupleDocProcessor(DocProcessor):
    def __init__(self, doc_cls_map, raw_result):
        super(NamedtupleDocProcessor, self).__init__(doc_cls_map, raw_result)
        # self._tuple_cls_map = {}
        # types_fields = collections.defaultdict(set)
        # for hit in raw_result['hits']['hits']:
        #     types_fields[hit['_type']].update(hit.keys())
        # for doc_type, fields in types_fields.items():
        #     self._tuple_cls_map[doc_type] = collections.namedtuple(
        #         '{}Doc'.format(to_camel_case(doc_type)),
        #         list(fields),
        #     )

    def process(self, hit, result):
        doc_cls = self._doc_cls_map.get(hit['_type'], DynamicDocument)
        return TupleDoc(doc_cls, hit)


class AbstractTuple(tuple):
    __slots__ = ()


class TupleDoc(tuple):
    def __new__(cls, doc_cls, hit):
        fields = []
        values = []
        for field_name, value in hit.items():
            attr_name, field_type = doc_cls._raw_field_names.get(field_name)
            if attr_name:
                fields.append(attr_name)
                # values.append(value)
                values.append(field_type.to_python(value))
            else:
                fields.append(field_name)
                values.append(value)
        t = tuple.__new__(cls, values)
        t.__dict__.update(zip(fields, values))
        t.__dict__['_fields'] = fields
        return t


class SearchResult(Result):
    def __init__(
            self, raw_result, aggregations=None,
            doc_cls=None, instance_mapper=None, doc_processor=None,
    ):
        super(SearchResult, self).__init__(raw_result)

        self._query_aggs = aggregations or {}

        if doc_cls is None:
            doc_classes = ()
        elif not isinstance(doc_cls, collections.Iterable):
            doc_classes = (doc_cls,)
        else:
            doc_classes = doc_cls
        self._doc_cls_map = {doc_cls.__doc_type__: doc_cls for doc_cls in doc_classes}

        self._mapper_registry = {}
        if isinstance(instance_mapper, dict):
            self._instance_mappers = instance_mapper
        else:
            self._instance_mappers = {doc_cls: instance_mapper for doc_cls in doc_classes}

        doc_processor_cls = doc_processor or DefaultDocProcessor
        doc_processor = doc_processor_cls(self._doc_cls_map, raw_result)
            
        self.error = raw_result.get('error')

        if not self.error or 'took' in raw_result:
            self.took = raw_result.get('took')
            
        if not self.error or 'timed_out' in raw_result:
            self.timed_out = raw_result.get('timed_out')
            
        if not self.error or 'hits' in raw_result:
            self.total = raw_result['hits']['total']
            self.max_score = raw_result['hits']['max_score']
            self.hits = []
            for hit in raw_result['hits']['hits']:
                self.hits.append(doc_processor.process(hit, self))

        if not self.error or 'aggregations' in raw_result:
            self.aggregations = {}
            for agg_name, agg_expr in self._query_aggs.items():
                raw_agg_data = raw_result['aggregations'][agg_name]
                agg_result = agg_expr.build_agg_result(raw_agg_data, self._doc_cls_map, mapper_registry=self._mapper_registry)
                self.aggregations[agg_name] = agg_result

        if not self.error or '_scroll_id' in raw_result:
            self.scroll_id = raw_result.get('_scroll_id')
            
    def __iter__(self):
        return iter(self.hits)

    def __getattr__(self, name):
        if self.error and name in ('took', 'timed_out', 'total', 'hits', 'max_score', 'aggregations', 'scroll_id'):
            raise DelayedElasticsearchException(self.error)
        return super(SearchResult, self).__getattr__(name)

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def _populate_instances(self, doc_cls):
        docs = [doc for doc in self.hits if isinstance(doc, doc_cls)]
        instances = self._instance_mappers.get(doc_cls)([doc._id for doc in docs])
        for doc in docs:
            doc.__dict__['instance'] = instances.get(doc._id)


class CountResult(Result):
    def __init__(self, raw_result):
        super(CountResult, self).__init__(raw_result)
        self.count = raw_result['count']


class ExistsResult(Result):
    def __init__(self, raw_result):
        super(ExistsResult, self).__init__(raw_result)
        self.exists = raw_result['exists']


class ActionResult(Result):
    def __init__(self, raw_result):
        super(ActionResult, self).__init__(raw_result)
        self.name = next(iter(raw_result.keys()))
        data = next(iter(raw_result.values()))
        self.status = data['status']
        self.found = data.get('found')
        raw_error = data.get('error')
        if raw_error:
            if isinstance(raw_error, string_types):
                self.error = raw_error
            else:
                self.error = ErrorReason(raw_error)
        else:
            self.error = None
        self._index = data['_index']
        self._type = data['_type']
        self._id = data['_id']
        self._version = data.get('_version')


class ErrorReason(object):
    def __init__(self, raw_error):
        self.type = raw_error['type']
        self.reason = raw_error['reason']


class BulkResult(Result):
    def __init__(self, raw_result):
        super(BulkResult, self).__init__(raw_result)
        self.took = raw_result['took']
        self.errors = raw_result['errors']
        self.items = list(map(ActionResult, raw_result['items']))

    def __iter__(self):
        return iter(self.items)


class DeleteResult(Result):
    def __init__(self, raw_result):
        super(DeleteResult, self).__init__(raw_result)
        self.found = raw_result['found']
        self._index = raw_result['_index']
        self._type = raw_result['_type']
        self._id = raw_result['_id']
        self._version = raw_result['_version']


class DeleteByQueryResult(Result):
    pass


class RefreshResult(Result):
    pass


class FlushResult(Result):
    pass
