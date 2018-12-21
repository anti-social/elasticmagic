from ...search import BaseSearchQuery
from ...util import cached_property


class AsyncSearchQuery(BaseSearchQuery):
    @cached_property
    async def _result(self):
        doc_cls = self._get_doc_cls()
        doc_type = self._get_doc_type(doc_cls)
        return await (self._index or self._cluster).search(
            self,
            doc_type=doc_type,
            **(self._search_params or {})
        )

    async def get_result(self):
        return await self._result
