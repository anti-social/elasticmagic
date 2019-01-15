from ...index import BaseIndex


class AsyncIndex(BaseIndex):
    async def get_compiler(self):
        return await self._cluster.get_compiler()

    async def get(
            self, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, preference=None, refresh=None,
            version=None, version_type=None, **kwargs
    ):
        return await self._cluster.get(
            self._name, id, doc_cls=doc_cls, doc_type=doc_type, source=source,
            realtime=realtime, routing=routing, preference=preference,
            refresh=refresh, version=version, version_type=version_type,
            **kwargs
        )

    async def multi_get(
            self, docs, doc_type=None, source=None, realtime=None,
            routing=None, preference=None, refresh=None, **kwargs
    ):
        return await self._cluster.multi_get(
            docs, index=self._name, doc_type=doc_type, source=source,
            realtime=realtime, routing=routing, preference=preference,
            refresh=refresh, **kwargs
        )

    mget = multi_get

    async def search(
            self, q, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        return await self._cluster.search(
            q, index=self._name, doc_type=doc_type,
            routing=routing, preference=preference, timeout=timeout,
            search_type=search_type, query_cache=query_cache,
            terminate_after=terminate_after, scroll=scroll,
            **kwargs
        )

    async def multi_search(
            self, queries, doc_type=None, routing=None, preference=None,
            search_type=None, **kwargs
    ):
        return await self._cluster.multi_search(
            queries, index=self._name, doc_type=doc_type,
            routing=routing, preference=preference, search_type=search_type,
            **kwargs
        )

    msearch = multi_search

    async def count(
            self, q=None, doc_type=None, routing=None, preference=None,
            **kwargs
    ):
        return await self._cluster.count(
            q, index=self._name, doc_type=doc_type, routing=routing,
            preference=preference, **kwargs
        )

    async def exists(
            self, q, doc_type=None, refresh=None, routing=None, **kwargs
    ):
        return await self._cluster.exists(
            q, index=self._name, doc_type=doc_type, refresh=refresh,
            routing=routing, **kwargs
        )

    async def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        return await self._cluster.scroll(
            scroll_id, scroll,
            doc_cls=doc_cls, instance_mapper=instance_mapper,
            **kwargs
        )

    async def clear_scroll(self, scroll_id, **kwargs):
        return await self._cluster.clear_scroll(scroll_id, **kwargs)

    async def put_mapping(
            self, doc_cls_or_mapping, doc_type=None, allow_no_indices=None,
            expand_wildcards=None, ignore_conflicts=None,
            ignore_unavailable=None, master_timeout=None, timeout=None,
            **kwargs
    ):
        return await self._cluster.put_mapping(
            doc_cls_or_mapping, index=self._name, doc_type=doc_type,
            allow_no_indices=allow_no_indices,
            expand_wildcards=expand_wildcards,
            ignore_conflicts=ignore_conflicts,
            ignore_unavailable=ignore_unavailable,
            master_timeout=master_timeout, timeout=timeout,
            **kwargs
        )

    async def add(
            self, docs, doc_type=None, refresh=None, timeout=None,
            consistency=None, replication=None, **kwargs
    ):
        return await self._cluster.add(
            docs, index=self._name, doc_type=doc_type, refresh=refresh,
            timeout=timeout, consistency=consistency, replication=replication,
            **kwargs
        )

    async def delete(
            self, doc_or_id, doc_cls=None, doc_type=None,
            timeout=None, consistency=None, replication=None,
            parent=None, routing=None, refresh=None, version=None,
            version_type=None,
            **kwargs
    ):
        return await self._cluster.delete(
            doc_or_id, index=self._name, doc_cls=doc_cls, doc_type=doc_type,
            timeout=timeout, consistency=consistency, replication=replication,
            parent=parent, routing=routing, refresh=refresh,
            version=version, version_type=version_type,
            **kwargs
        )

    async def delete_by_query(
            self, q, doc_type=None, timeout=None, consistency=None,
            replication=None, routing=None, **kwargs
    ):
        return await self._cluster.delete_by_query(
            q, index=self._name, doc_type=doc_type,
            timeout=timeout, consistency=consistency,
            replication=replication, routing=routing,
            **kwargs
        )

    async def bulk(self, actions, doc_type=None, refresh=None, **kwargs):
        return await self._cluster.bulk(
            actions, index=self._name, doc_type=doc_type, refresh=refresh,
            **kwargs
        )

    async def refresh(self, **kwargs):
        return await self._cluster.refresh(index=self._name, **kwargs)

    async def flush(self, **kwargs):
        return await self._cluster.flush(index=self._name, **kwargs)
