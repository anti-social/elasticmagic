import collections

from .compat import string_types
from .document import DynamicDocument


class Result(object):
    def __init__(self, raw_result):
        self.raw = raw_result


class SearchResult(Result):
    def __init__(
            self, raw_result, aggregations=None, doc_cls=None,
            instance_mapper=None,
    ):
        super(SearchResult, self).__init__(raw_result)

        self._query_aggs = aggregations or {}

        if doc_cls is None:
            doc_classes = ()
        elif not isinstance(doc_cls, collections.Iterable):
            doc_classes = (doc_cls,)
        else:
            doc_classes = doc_cls
        self._doc_cls_map = {
            doc_cls.__doc_type__: doc_cls for doc_cls in doc_classes
        }

        self._mapper_registry = {}
        if isinstance(instance_mapper, dict):
            self._instance_mappers = instance_mapper
        else:
            self._instance_mappers = {
                doc_cls: instance_mapper for doc_cls in doc_classes
            }

        self.error = raw_result.get('error')
        self.took = raw_result.get('took')
        self.timed_out = raw_result.get('timed_out')

        hits = raw_result.get('hits') or {}
        self.total = hits.get('total')
        self.max_score = hits.get('max_score')
        self.hits = []
        for hit in hits.get('hits', []):
            doc_cls = self._doc_cls_map.get(hit['_type'], DynamicDocument)
            self.hits.append(doc_cls(_hit=hit, _result=self))

        self.aggregations = {}
        for agg_name, agg_expr in self._query_aggs.items():
            raw_agg_data = raw_result.get('aggregations', {}).get(agg_name, {})
            agg_result = agg_expr.build_agg_result(
                raw_agg_data, self._doc_cls_map,
                mapper_registry=self._mapper_registry
            )
            self.aggregations[agg_name] = agg_result

        self.scroll_id = raw_result.get('_scroll_id')

    def __iter__(self):
        return iter(self.hits)

    def __len__(self):
        return len(self.hits)

    def get_aggregation(self, name):
        return self.aggregations.get(name)

    def _populate_instances(self, doc_cls):
        docs = [doc for doc in self.hits if isinstance(doc, doc_cls)]
        instances = self._instance_mappers.get(doc_cls)(
            [doc._id for doc in docs]
        )
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
        self.found = raw_result.get('found')
        self.result = raw_result.get('result')
        self._index = raw_result['_index']
        self._type = raw_result['_type']
        self._id = raw_result['_id']
        self._version = raw_result['_version']


class DeleteByQueryResult(Result):
    def __init__(self, raw_result):
        super(DeleteByQueryResult, self).__init__(raw_result)
        self.took = raw_result.get('took')
        self.timed_out = raw_result.get('timed_out')
        self.deleted = raw_result.get('deleted')
        self.batches = raw_result.get('batches')
        self.version_conflicts = raw_result.get('version_conflicts')
        self.noops = raw_result.get('noops')
        self.retries = self.Retries(raw_result.get('retries') or {})
        self.throttled_millis = raw_result.get('throttled_millis')
        self.requests_per_second = raw_result.get('requests_per_second')
        self.throttled_until_millis = raw_result.get('throttled_until_millis')
        self.total = raw_result.get('total')
        self.failures = raw_result.get('failures')

    class Retries(object):
        def __init__(self, raw_result):
            self.bulk = raw_result.get('bulk')
            self.search = raw_result.get('search')


class RefreshResult(Result):
    pass


class FlushResult(Result):
    pass


class ClearScrollResult(Result):
    def __init__(self, raw_result):
        super(ClearScrollResult, self).__init__(raw_result)
        self.succeeded = raw_result.get('succeeded')
        self.num_freed = raw_result.get('num_freed')
