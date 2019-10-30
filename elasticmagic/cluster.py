from abc import ABCMeta

from .compat import with_metaclass
from .compiler import (
    ESVersion,
    get_compiler_by_es_version,
)
from .index import Index
from .result import (
    ClearScrollResult,
    FlushResult,
    RefreshResult,
)
from .search import SearchQuery
from .util import clean_params

MAX_RESULT_WINDOW = 10000


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

    def _preprocess_params(self, params, *pop_keys):
        params = params.copy()
        params.pop('self')
        kwargs = params.pop('kwargs') or {}
        for key in pop_keys:
            params.pop(key)
        return clean_params(params, **kwargs)

    def _get_params(self, params):
        return self._preprocess_params(params, 'doc_or_id', 'doc_cls')

    def _multi_get_params(self, params):
        return self._preprocess_params(params, 'docs_or_ids', 'doc_cls')

    def _search_params(self, params):
        return self._preprocess_params(params, 'q')

    def _scroll_params(self, params):
        return self._preprocess_params(params, 'doc_cls', 'instance_mapper')

    def _clear_scroll_result(self, raw_result):
        return ClearScrollResult(raw_result)

    def _multi_search_params(self, params):
        params = self._preprocess_params(params, 'queries')
        raise_on_error = params.pop(
            'raise_on_error', self._multi_search_raise_on_error
        )
        return params, raise_on_error

    def _put_mapping_params(self, params):
        return self._preprocess_params(params, 'doc_cls_or_mapping')

    def _create_index_params(self, params):
        return self._preprocess_params(params, 'settings', 'mappings')

    def _add_params(self, params):
        from . import actions

        params = self._preprocess_params(params)
        docs = params.pop('docs')
        return (
            [actions.Index(d) for d in docs],
            params
        )

    def _delete_params(self, params):
        return self._preprocess_params(params, 'doc_or_id', 'doc_cls')

    def _bulk_params(self, params):
        return self._preprocess_params(params, 'actions')

    def _refresh_result(self, raw_result):
        return RefreshResult(raw_result)

    def _flush_result(self, raw_result):
        return FlushResult(raw_result)


class Cluster(BaseCluster):
    _index_cls = Index
    _search_query_cls = SearchQuery

    def _do_request(self, compiler, *args, **kwargs):
        compiled_query = compiler(*args, **kwargs)
        api_method = compiled_query.api_method(self._client)
        if compiled_query.body is None:
            raw_res = api_method(**compiled_query.params)
        else:
            raw_res = api_method(
                body=compiled_query.body, **compiled_query.params
            )
        return compiled_query.process_result(raw_res)

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
            doc_or_id, self._get_params(locals()), doc_cls=doc_cls
        )

    def multi_get(
            self, docs_or_ids, index=None, doc_cls=None, doc_type=None,
            source=None, parent=None, routing=None, preference=None,
            realtime=None, refresh=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_multi_get,
            docs_or_ids, self._multi_get_params(locals()), doc_cls=doc_cls
        )

    mget = multi_get

    def search(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_search_query,
            q, self._search_params(locals())
        )

    def count(
            self, q=None, index=None, doc_type=None, routing=None,
            preference=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_count_query,
            q, self._search_params(locals())
        )

    def exists(
            self, q=None, index=None, doc_type=None, refresh=None,
            routing=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_exists_query,
            q, self._search_params(locals())
        )

    def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_scroll,
            self._scroll_params(locals()),
            doc_cls=doc_cls, instance_mapper=instance_mapper
        )

    def clear_scroll(self, scroll_id, **kwargs):
        params = self._preprocess_params(locals())
        return self._clear_scroll_result(
            self._client.clear_scroll(**params)
        )

    def multi_search(
            self, queries, index=None, doc_type=None,
            routing=None, preference=None, search_type=None,
            raise_on_error=None, **kwargs
    ):
        params, raise_on_error = self._multi_search_params(locals())
        return self._do_request(
            self.get_compiler().compiled_multi_search,
            queries, params, raise_on_error=raise_on_error
        )

    msearch = multi_search

    def put_mapping(
            self, doc_cls_or_mapping, index=None, doc_type=None,
            allow_no_indices=None, expand_wildcards=None,
            ignore_conflicts=None, ignore_unavailable=None,
            master_timeout=None, timeout=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_put_mapping,
            doc_cls_or_mapping, self._put_mapping_params(locals())
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
        return self._do_request(
            self.get_compiler().compiled_delete,
            doc_or_id, self._delete_params(locals()), doc_cls=doc_cls
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
            q, self._search_params(locals())
        )

    def bulk(
            self, actions, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        return self._do_request(
            self.get_compiler().compiled_bulk,
            actions, self._bulk_params(locals())
        )

    def refresh(self, index=None, **kwargs):
        params = self._preprocess_params(locals())
        return self._refresh_result(
            self._client.indices.refresh(**params)
        )

    def flush(self, index=None, **kwargs):
        params = self._preprocess_params(locals())
        return self._flush_result(
            self._client.indices.flush(**params)
        )

    def flush_synced(self, index=None, **kwargs):
        params = self._preprocess_params(locals())
        return self._flush_result(
            self._client.indices.flush_synced(**params)
        )
