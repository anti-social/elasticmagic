from abc import ABCMeta, abstractmethod

from . import api
from .compat import with_metaclass
from .compiler import DefaultCompiler, ESVersion, get_compiler_by_es_version
from .index import Index
from .search import SearchQuery

MAX_RESULT_WINDOW = 10000


class BaseCluster(with_metaclass(ABCMeta)):
    def __init__(
            self, client,
            multi_search_raise_on_error=True, compiler=None,
            index_cls=None, sniff_elastic_version=False,
    ):
        self._client = client
        self._multi_search_raise_on_error = multi_search_raise_on_error
        self._compiler = compiler or DefaultCompiler()
        self._index_cls = index_cls or Index
        self._sniff_elastic_version = sniff_elastic_version
        self._index_cache = {}
        self._es_version = None

    def __getitem__(self, index_name):
        return self.get_index(index_name)

    def _get_compiler(self):
        if self._sniff_elastic_version:
            return get_compiler_by_es_version(self.get_es_version())
        else:
            return self._compiler

    def get_index(self, name):
        if isinstance(name, tuple):
            name = ','.join(name)

        if name not in self._index_cache:
            self._index_cache[name] = self._index_cls(self, name)
        return self._index_cache[name]

    def get_client(self):
        return self._client

    @abstractmethod
    def search_query(self, *args, **kwargs):
        pass

    def query(self, *args, **kwargs):
        return self.search_query(*args, **kwargs)

    def _es_version_result(self, raw_result):
        version_str = raw_result['version']['number']
        version_str, _, snapshot = version_str.partition('-')
        major, minor, patch = map(int, version_str.split('.'))
        return ESVersion(major, minor, patch)


class Cluster(BaseCluster):
    def search_query(self, *args, **kwargs):
        kwargs['cluster'] = self
        kwargs.setdefault('_compiler', self._compiler.compiled_query)
        return SearchQuery(*args, **kwargs)

    def get_es_version(self):
        if not self._es_version:
            self._es_version = self._es_version_result(
                self._client.info()
            )
        return self._es_version

    def get(
            self, index, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, parent=None, preference=None,
            refresh=None, version=None, version_type=None, **kwargs
    ):
        doc_cls, params = api.get_params(locals())
        return api.get_result(
            doc_cls,
            self._client.get(**params),
        )

    def multi_get(
            self, docs, index=None, doc_type=None, source=None,
            parent=None, routing=None, preference=None, realtime=None,
            refresh=None, **kwargs
    ):
        doc_classes, params = api.multi_get_params(locals())
        return api.multi_get_result(
            doc_classes,
            self._client.mget(**params),
        )

    mget = multi_get

    def search(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        body, params = api.search_params(locals())
        return api.search_result(
            q,
            self._client.search(body=body, **params),
        )

    def count(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            **kwargs
    ):
        body, params = api.count_params(locals())
        return api.count_result(
            self._client.count(body=body, **params)
        )

    def exists(
            self, q, index=None, doc_type=None, refresh=None, routing=None,
            **kwargs
    ):
        body, params = api.exists_params(locals())
        return api.exists_result(
            self._client.search_exists(body=body, **params)
        )

    def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        doc_cls, instance_mapper, params = api.scroll_params(locals())
        return api.scroll_result(
            doc_cls,
            instance_mapper,
            self._client.scroll(**params)
        )

    def clear_scroll(self, scroll_id, **kwargs):
        params = api.clear_scroll_params(locals())
        return api.clear_scroll_result(
            self._client.clear_scroll(body=scroll_id, **kwargs)
        )

    def multi_search(
            self, queries, index=None, doc_type=None,
            routing=None, preference=None, search_type=None,
            raise_on_error=None, **kwargs
    ):
        body, raise_on_error, params = api.multi_search_params(locals())
        return api.multi_search_result(
            queries,
            raise_on_error,
            self._client.msearch(body=body, **params)['responses'],
        )

    msearch = multi_search

    def put_mapping(
            self, doc_cls_or_mapping, index, doc_type=None,
            allow_no_indices=None, expand_wildcards=None,
            ignore_conflicts=None, ignore_unavailable=None,
            master_timeout=None, timeout=None, **kwargs
    ):
        body, params = api.put_mapping_params(locals())
        return api.put_mapping_result(
            self._client.indices.put_mapping(body=body, **params)
        )

    def add(
            self, docs, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        actions, params = api.add_params(locals())
        return self.bulk(actions, **params)

    def delete(
            self, doc_or_id, index, doc_cls=None, doc_type=None,
            timeout=None, consistency=None, replication=None,
            parent=None, routing=None, refresh=None, version=None,
            version_type=None,
            **kwargs
    ):
        params = api.delete_params(locals())
        return api.delete_result(
            self._client.delete(**params)
        )

    def delete_by_query(
            self, q, index=None, doc_type=None,
            timeout=None, consistency=None, replication=None, routing=None,
            **kwargs
    ):
        params = api.delete_by_query_params(locals())
        return api.delete_result(
            self._client.delete_by_query(**params)
        )

    def bulk(
            self, actions, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        params = api.bulk_params(locals())
        return api.bulk_result(
            self._client.bulk(**params)
        )

    def refresh(self, index=None, **kwargs):
        params = api.refresh_params(locals())
        return api.refresh_result(
            self._client.indices.refresh(**params)
        )

    def flush(self, index=None, **kwargs):
        params = api.flush_params(locals())
        return api.flush_result(
            self._client.indices.flush(**params)
        )

    def flush_synced(self, index=None, **kwargs):
        params = api.flush_params(locals())
        return api.flush_result(
            self._client.indices.flush_synced(**params)
        )
