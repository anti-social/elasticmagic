from abc import ABCMeta
from collections import Iterable, Mapping

from .compat import with_metaclass
from .compiler import (
    ESVersion,
    get_compiler_by_es_version,
)
from .document import (
    Document,
    DynamicDocument,
)
from .index import Index
from .result import (
    BulkResult,
    ClearScrollResult,
    DeleteResult,
    FlushResult,
    RefreshResult,
    SearchResult,
)
from .search import SearchQuery
from .util import clean_params

MAX_RESULT_WINDOW = 10000


def _preprocess_params(params):
    params = params.copy()
    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    return params, kwargs


class BaseCluster(with_metaclass(ABCMeta)):
    _index_cls = None
    _search_query_cls = None

    def __init__(
            self, client, index_cls=None,
            multi_search_raise_on_error=True,
            autodetect_es_version=True, compiler=None,
    ):
        self._client = client
        self._index_cls = index_cls or self._index_cls
        self._multi_search_raise_on_error = multi_search_raise_on_error
        assert autodetect_es_version or compiler, (
            'Cannot detect compiler: either `autodetect_es_version` should be '
            '`True` or `compiler` must be specified'
        )
        self._autodetect_es_version = autodetect_es_version
        self._compiler = compiler
        self._index_cache = {}
        self._es_version = None

    def __getitem__(self, index_name):
        return self.get_index(index_name)

    def get_index(self, name):
        if isinstance(name, tuple):
            name = ','.join(name)

        if name not in self._index_cache:
            self._index_cache[name] = self._index_cls(self, name)
        return self._index_cache[name]

    def get_client(self):
        return self._client

    def search_query(self, *args, **kwargs):
        """Returns a :class:`search.SearchQuery` instance that is bound to this
        cluster.
        """
        kwargs['cluster'] = self
        return self._search_query_cls(*args, **kwargs)

    def query(self, *args, **kwargs):
        return self.search_query(*args, **kwargs)

    def _es_version_result(self, raw_result):
        version_str = raw_result['version']['number']
        version_str, _, snapshot = version_str.partition('-')
        major, minor, patch = map(int, version_str.split('.'))
        return ESVersion(major, minor, patch)

    def _get_params(self, params):
        params, kwargs = _preprocess_params(params)
        params.pop('doc_or_id')
        return clean_params(params, **kwargs)

    def _multi_get_params(self, params):
        params, kwargs = _preprocess_params(params)
        docs_or_ids = params.pop('docs')
        doc_cls = params.pop('doc_cls', None) or DynamicDocument
        if (
                params.get('doc_type') is None and
                getattr(doc_cls, '__doc_type__', None)
        ):
            params['doc_type'] = doc_cls.__doc_type__

        params = clean_params(params, **kwargs)
        body = {}
        body['docs'] = []
        doc_classes = []
        for doc_or_id in docs_or_ids:
            if isinstance(doc_or_id, Document):
                body['docs'].append(doc_or_id.to_meta())
                doc_classes.append(doc_or_id.__class__)
            elif isinstance(doc_or_id, Mapping):
                body['docs'].append(doc_or_id)
                doc_classes.append(doc_or_id.pop('doc_cls', None))
            else:
                body['docs'].append({'_id': doc_or_id})
                doc_classes.append(None)
        params['body'] = body
        return doc_classes, doc_cls, params

    def _multi_get_result(self, doc_classes, default_doc_cls, raw_result):
        result_docs = []
        if isinstance(default_doc_cls, Iterable):
            doc_cls_map = {
                doc_cls.__doc_type__: doc_cls for doc_cls in default_doc_cls
            }
            default_doc_cls = None
        else:
            doc_cls_map = None

        for doc_cls, raw_doc in zip(doc_classes, raw_result['docs']):
            if doc_cls is None and doc_cls_map:
                doc_cls = doc_cls_map.get(raw_doc['_type'])
            if doc_cls is None and default_doc_cls:
                doc_cls = default_doc_cls

            if raw_doc['found']:
                result_docs.append(doc_cls(_hit=raw_doc))
            else:
                result_docs.append(None)

        return result_docs

    def _search_params(self, params):
        params, kwargs = _preprocess_params(params)
        params.pop('q')
        return clean_params(params, **kwargs)

    def _scroll_params(self, params):
        params, kwargs = _preprocess_params(params)
        doc_cls = params.pop('doc_cls', None)
        instance_mapper = params.pop('instance_mapper', None)
        return doc_cls, instance_mapper, clean_params(params, **kwargs)

    def _scroll_result(self, doc_cls, instance_mapper, raw_result):
        return SearchResult(
            raw_result,
            doc_cls=doc_cls,
            instance_mapper=instance_mapper,
        )

    def _clear_scroll_params(self, params):
        params, kwargs = _preprocess_params(params)
        return clean_params(params, **kwargs)

    def _clear_scroll_result(self, raw_result):
        return ClearScrollResult(raw_result)

    def _multi_search_params(self, params):
        params, kwargs = _preprocess_params(params)
        params.pop('queries')
        if params.get('raise_on_error') is None:
            params['raise_on_error'] = self._multi_search_raise_on_error
        return clean_params(params, **kwargs)

    def _put_mapping_params(self, params):
        params, kwargs = _preprocess_params(params)
        doc_cls_or_mapping = params.pop('doc_cls_or_mapping')
        if issubclass(doc_cls_or_mapping, Document):
            body = doc_cls_or_mapping.to_mapping()
        else:
            body = doc_cls_or_mapping
        if params.get('doc_type', None) is None:
            params['doc_type'] = getattr(
                doc_cls_or_mapping, '__doc_type__', None
            )
        return body, clean_params(params, **kwargs)

    def _put_mapping_result(self, raw_result):
        # TODO Convert to nice result object
        return raw_result

    def _add_params(self, params):
        from . import actions

        params, kwargs = _preprocess_params(params)
        docs = params.pop('docs')

        # TODO: Override an index for action if there is index in params
        return [actions.Index(d) for d in docs], clean_params(params, **kwargs)

    def _delete_params(self, params):
        params, kwargs = _preprocess_params(params)
        doc_or_id = params.pop('doc_or_id')
        doc_cls = params.pop('doc_cls', None)
        doc_type = params.pop('doc_type', None)
        if isinstance(doc_or_id, Document):
            doc_id = doc_or_id._id
            doc_cls = doc_cls or doc_or_id.__class__
        else:
            doc_id = doc_or_id
        assert doc_type or (doc_cls and doc_cls.__doc_type__), \
            'Cannot evaluate doc_type: you must specify doc_type or doc_cls'
        params['doc_type'] = doc_type or doc_cls.__doc_type__
        params['id'] = doc_id
        return clean_params(params, **kwargs)

    def _delete_result(self, raw_result):
        return DeleteResult(raw_result)

    def _bulk_params(self, params):
        params, kwargs = _preprocess_params(params)
        actions = params.pop('actions')
        body = []
        for act in actions:
            body.append({act.__action_name__: act.get_meta()})
            source = act.get_source()
            if source is not None:
                body.append(source)
        return clean_params(params, body=body, **kwargs)

    def _bulk_result(self, raw_result):
        return BulkResult(raw_result)

    def _refresh_params(self, params):
        params, kwargs = _preprocess_params(params)
        index = params.pop('index')
        return clean_params(params, index=index, **kwargs)

    def _refresh_result(self, raw_result):
        return RefreshResult(raw_result)

    def _flush_params(self, params):
        params, kwargs = _preprocess_params(params)
        index = params.pop('index')
        return clean_params(params, index=index, **kwargs)

    def _flush_result(self, raw_result):
        return FlushResult(raw_result)


class Cluster(BaseCluster):
    _index_cls = Index
    _search_query_cls = SearchQuery

    def _do_request(self, compiler, *args, **kwargs):
        compiled_query = compiler(*args, **kwargs)
        api_method = getattr(self._client, compiled_query.api_method)
        if compiled_query.body is None:
            return compiled_query.process_result(
                api_method(**compiled_query.params)
            )
        return compiled_query.process_result(
            api_method(body=compiled_query.body, **compiled_query.params)
        )

    def get_compiler(self):
        if self._compiler:
            return self._compiler
        else:
            return get_compiler_by_es_version(self.get_es_version())

    def get_es_version(self):
        if not self._es_version:
            self._es_version = self._es_version_result(
                self._client.info()
            )
        return self._es_version

    def get(
            self, doc_or_id, index=None, doc_cls=None, doc_type=None,
            routing=None, source=None, realtime=None, parent=None,
            preference=None, refresh=None, version=None, version_type=None,
            **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_get,
            doc_or_id, **self._get_params(locals())
        )

    def multi_get(
            self, docs, index=None, doc_cls=None, doc_type=None, source=None,
            parent=None, routing=None, preference=None, realtime=None,
            refresh=None, **kwargs
    ):
        doc_classes, default_doc_cls, params = self._multi_get_params(locals())
        return self._multi_get_result(
            doc_classes,
            default_doc_cls,
            self._client.mget(**params),
        )

    mget = multi_get

    def search(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_search_query,
            q, **self._search_params(locals())
        )

    def count(
            self, q=None, index=None, doc_type=None, routing=None,
            preference=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_count_query,
            q, **self._search_params(locals())
        )

    def exists(
            self, q=None, index=None, doc_type=None, refresh=None,
            routing=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_exists_query,
            q, **self._search_params(locals())
        )

    def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        doc_cls, instance_mapper, params = self._scroll_params(locals())
        return self._scroll_result(
            doc_cls,
            instance_mapper,
            self._client.scroll(**params)
        )

    def clear_scroll(self, scroll_id, **kwargs):
        params = self._clear_scroll_params(locals())
        return self._clear_scroll_result(
            self._client.clear_scroll(**params)
        )

    def multi_search(
            self, queries, index=None, doc_type=None,
            routing=None, preference=None, search_type=None,
            raise_on_error=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_multi_search,
            queries, **self._multi_search_params(locals())
        )

    msearch = multi_search

    def put_mapping(
            self, doc_cls_or_mapping, index, doc_type=None,
            allow_no_indices=None, expand_wildcards=None,
            ignore_conflicts=None, ignore_unavailable=None,
            master_timeout=None, timeout=None, **kwargs
    ):
        body, params = self._put_mapping_params(locals())
        return self._put_mapping_result(
            self._client.indices.put_mapping(body=body, **params)
        )

    def add(
            self, docs, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        actions, params = self._add_params(locals())
        return self.bulk(actions, **params)

    def delete(
            self, doc_or_id, index, doc_cls=None, doc_type=None,
            timeout=None, consistency=None, replication=None,
            parent=None, routing=None, refresh=None, version=None,
            version_type=None,
            **kwargs
    ):
        params = self._delete_params(locals())
        return self._delete_result(
            self._client.delete(**params)
        )

    def delete_by_query(
            self, q, index=None, doc_type=None, routing=None,
            conflicts=None, refresh=None, timeout=None,
            scroll=None, scroll_size=None,
            wait_for_completion=None, requests_per_second=None,
            **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_delete_by_query,
            q, **self._search_params(locals())
        )

    def bulk(
            self, actions, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        params = self._bulk_params(locals())
        return self._bulk_result(
            self._client.bulk(**params)
        )

    def refresh(self, index=None, **kwargs):
        params = self._refresh_params(locals())
        return self._refresh_result(
            self._client.indices.refresh(**params)
        )

    def flush(self, index=None, **kwargs):
        params = self._flush_params(locals())
        return self._flush_result(
            self._client.indices.flush(**params)
        )

    def flush_synced(self, index=None, **kwargs):
        params = self._flush_params(locals())
        return self._flush_result(
            self._client.indices.flush_synced(**params)
        )
