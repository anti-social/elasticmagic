from elasticmagic.compiler import get_compiler_by_es_version

from ...cluster import BaseCluster
from .index import AsyncIndex
from .search import AsyncSearchQuery


class AsyncCluster(BaseCluster):
    _index_cls = AsyncIndex
    _search_query_cls = AsyncSearchQuery

    async def get_es_version(self):
        if not self._es_version:
            self._es_version = self._es_version_result(
                await self._client.info()
            )
        return self._es_version

    async def get_compiler(self):
        if self._compiler:
            return self._compiler
        else:
            return get_compiler_by_es_version(await self.get_es_version())

    async def get(
            self, index, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, parent=None, preference=None,
            refresh=None, version=None, version_type=None, **kwargs
    ):
        doc_cls, params = self._get_params(locals())
        return self._get_result(
            doc_cls,
            await self._client.get(**params),
        )

    async def multi_get(
            self, docs, index=None, doc_cls=None, doc_type=None, source=None,
            parent=None, routing=None, preference=None, realtime=None,
            refresh=None, **kwargs
    ):
        doc_classes, default_doc_cls, params = self._multi_get_params(locals())
        return self._multi_get_result(
            doc_classes,
            default_doc_cls,
            await self._client.mget(**params),
        )

    mget = multi_get

    async def search(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        body, params = self._search_params(
            locals(), await self.get_compiler()
        )
        return self._search_result(
            q,
            await self._client.search(body=body, **params),
        )

    async def count(
            self, q=None, index=None, doc_type=None, routing=None,
            preference=None, **kwargs
    ):
        body, params = self._count_params(locals(), await self.get_compiler())
        return self._count_result(
            await self._client.count(body=body, **params)
        )

    async def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        doc_cls, instance_mapper, params = self._scroll_params(locals())
        return self._scroll_result(
            doc_cls,
            instance_mapper,
            await self._client.scroll(**params)
        )

    async def clear_scroll(self, scroll_id, **kwargs):
        params = self._clear_scroll_params(locals())
        return self._clear_scroll_result(
            await self._client.clear_scroll(**params)
        )

    async def multi_search(
            self, queries, index=None, doc_type=None,
            routing=None, preference=None, search_type=None,
            raise_on_error=None, **kwargs
    ):
        body, raise_on_error, params = self._multi_search_params(
            locals(), await self.get_compiler()
        )
        return self._multi_search_result(
            queries,
            raise_on_error,
            (await self._client.msearch(body=body, **params))['responses'],
        )

    msearch = multi_search

    async def put_mapping(
            self, doc_cls_or_mapping, index, doc_type=None,
            allow_no_indices=None, expand_wildcards=None,
            ignore_conflicts=None, ignore_unavailable=None,
            master_timeout=None, timeout=None, **kwargs
    ):
        mapping, params = self._put_mapping_params(locals())
        return self._put_mapping_result(
            await self._client.indices.put_mapping(body=mapping, **params)
        )

    async def add(
            self, docs, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        actions, params = self._add_params(locals())
        return await self.bulk(actions, **params)

    async def delete(
            self, doc_or_id, index, doc_cls=None, doc_type=None,
            timeout=None, consistency=None, replication=None,
            parent=None, routing=None, refresh=None, version=None,
            version_type=None,
            **kwargs
    ):
        params = self._delete_params(locals())
        return self._delete_result(
            await self._client.delete(**params)
        )

    async def delete_by_query(
            self, q, index=None, doc_type=None,
            timeout=None, consistency=None, replication=None, routing=None,
            **kwargs
    ):
        params = self._delete_by_query_params(
            locals(), await self.get_compiler()
        )
        return self._delete_by_query_result(
            await self._client.delete_by_query(**params)
        )

    async def bulk(
            self, actions, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        params = self._bulk_params(locals())
        return self._bulk_result(
            await self._client.bulk(**params)
        )

    async def refresh(self, index=None, **kwargs):
        params = self._refresh_params(locals())
        return self._refresh_result(
            await self._client.indices.refresh(**params)
        )

    async def flush(self, index=None, **kwargs):
        params = self._flush_params(locals())
        return self._flush_result(
            await self._client.indices.flush(**params)
        )
