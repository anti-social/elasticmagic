from elasticmagic.ext.pagination import BaseSearchQueryWrapper


class AsyncSearchQueryWrapper(BaseSearchQueryWrapper):

    async def _getitem_async(self, k):
        self._prepare_getitem(k)
        self.items = list(await self.sliced_query)
        self.count = (await self.sliced_query.get_result()).total
        return self.items

    def __getitem__(self, k):
        return self._getitem_async(k)

    def __await__(self):
        if self.items is None:
            raise ValueError('Slice first')
        return self.sliced_query.__await__()

    def __len__(self):
        if self.count is None:
            raise ValueError('Slice first')
        return self.count

    async def get_result(self):
        if self.sliced_query is None:
            raise ValueError('Slice first')
        return await self.sliced_query.get_result()
