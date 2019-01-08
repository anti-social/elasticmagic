from ... import api
from ...cluster import BaseCluster
from .index import AsyncIndex
from .search import AsyncSearchQuery


class AsyncCluster(BaseCluster):
    def __init__(
            self, client,
            multi_search_raise_on_error=True, compiler=None,
            index_cls=None, sniff_elastic_version=False,
    ):
        super(AsyncCluster, self).__init__(
            client,
            multi_search_raise_on_error=multi_search_raise_on_error,
            compiler=compiler,
            index_cls=index_cls or AsyncIndex,
            sniff_elastic_version=sniff_elastic_version,
        )

    def search_query(self, *args, **kwargs):
        kwargs['cluster'] = self
        kwargs.setdefault('_compiler', self._compiler.get_query_compiler())
        return AsyncSearchQuery(*args, **kwargs)

    async def get_es_version(self):
        if not self._es_version:
            self._es_version = self._es_version_result(
                await self._client.info()
            )
        return self._es_version

    async def get(
            self, index, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, parent=None, preference=None,
            refresh=None, version=None, version_type=None, **kwargs
    ):
        doc_cls, params = api.get_params(locals())
        return api.get_result(
            doc_cls,
            await self._client.get(**params),
        )

    async def multi_get(
            self, docs, index=None, doc_type=None, source=None,
            parent=None, routing=None, preference=None, realtime=None,
            refresh=None, **kwargs
    ):
        doc_classes, params = api.multi_get_params(locals())
        return api.multi_get_result(
            doc_classes,
            await self._client.mget(**params),
        )

    mget = multi_get

    async def search(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        body, params = api.search_params(locals())
        return api.search_result(
            q,
            await self._client.search(body=body, **params),
        )

    async def count(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            **kwargs
    ):
        body, params = api.count_params(locals())
        return api.count_result(
            await self._client.count(body=body, **params)
        )

    async def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        doc_cls, instance_mapper, params = api.scroll_params(locals())
        return api.scroll_result(
            doc_cls,
            instance_mapper,
            await self._client.scroll(**params)
        )

    async def clear_scroll(self, scroll_id, **kwargs):
        params = api.clear_scroll_params(locals())
        return api.clear_scroll_result(
            await self._client.clear_scroll(**params)
        )

    async def multi_search(
            self, queries, index=None, doc_type=None,
            routing=None, preference=None, search_type=None,
            raise_on_error=None, **kwargs
    ):
        body, raise_on_error, params = api.multi_search_params(locals())
        return api.multi_search_result(
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
        mapping, params = api.put_mapping_params(locals())
        return api.put_mapping_result(
            await self._client.indices.put_mapping(body=mapping, **params)
        )

    async def add(
            self, docs, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        actions, params = api.add_params(locals())
        return await self.bulk(actions, **params)

    async def delete(
            self, doc_or_id, index, doc_cls=None, doc_type=None,
            timeout=None, consistency=None, replication=None,
            parent=None, routing=None, refresh=None, version=None,
            version_type=None,
            **kwargs
    ):
        params = api.delete_params(locals())
        return api.delete_result(
            await self._client.delete(**params)
        )

    async def delete_by_query(
            self, q, index=None, doc_type=None,
            timeout=None, consistency=None, replication=None, routing=None,
            **kwargs
    ):
        params = api.delete_by_query_params(locals())
        return api.delete_by_query_result(
            await self._client.delete_by_query(**params)
        )

    async def bulk(
            self, actions, index=None, doc_type=None, refresh=None,
            timeout=None, consistency=None, replication=None, **kwargs
    ):
        params = api.bulk_params(locals())
        return api.bulk_result(
            await self._client.bulk(**params)
        )

    async def refresh(self, index=None, **kwargs):
        params = api.refresh_params(locals())
        return api.refresh_result(
            await self._client.indices.refresh(**params)
        )

    async def flush(self, index=None, **kwargs):
        params = api.flush_params(locals())
        return api.flush_result(
            await self._client.indices.flush(**params)
        )
