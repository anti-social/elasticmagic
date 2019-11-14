from ...search import BaseSearchQuery


class AsyncSearchQuery(BaseSearchQuery):
    """Asynchronous version of the :class:`.SearchQuery`
    """

    async def to_dict(self, compiler=None):
        compiler = compiler or await self.get_compiler()
        return compiler.compiled_query(self).body

    async def get_compiler(self):
        return await self._index_or_cluster.get_compiler()

    async def get_query_compiler(self):
        return (await self.get_compiler()).compiled_query

    async def get_result(self):
        if self._cached_result is not None:
            return self._cached_result

        self._cached_result = await self._index_or_cluster.search(self)
        return self._cached_result

    async def count(self):
        return (
            await self._index_or_cluster.count(self)
        ).count

    async def exists(self):
        return (
            await self._index_or_cluster.exists(self)
        ).exists

    async def explain(self, doc, **kwargs):
        return await self._index_or_cluster.explain(self, doc, **kwargs)

    async def delete(
            self, conflicts=None, refresh=None, timeout=None,
            scroll=None, scroll_size=None,
            wait_for_completion=None, requests_per_second=None,
            **kwargs
    ):
        return await self._index_or_cluster.delete_by_query(
            self,
            conflicts=conflicts,
            refresh=refresh,
            timeout=timeout,
            scroll=scroll,
            scroll_size=scroll_size,
            wait_for_completion=wait_for_completion,
            requests_per_second=requests_per_second,
            **kwargs
        )

    async def _iter_result_async(self):
        return self._iter_result(await self.get_result())

    def __await__(self):
        return self._iter_result_async().__await__()

    async def _getitem_async(self, k):
        clone, is_slice = self._prepare_slice(k)
        if is_slice:
            return list(await clone)
        else:
            return list(await clone)[0]

    def __getitem__(self, k):
        return self._getitem_async(k)
