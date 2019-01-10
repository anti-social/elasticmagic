from elasticmagic.cluster import MAX_RESULT_WINDOW

from . import AsyncSearchQueryWrapper
from ...pagination.flask import BasePagination


class AsyncPagination(BasePagination):
    """Helper class to provide compatibility with Flask-SQLAlchemy paginator.
    """

    @classmethod
    async def create(
            cls, query, page=1, per_page=10, max_items=MAX_RESULT_WINDOW
    ):
        self = cls()
        self.original_query = query
        self.query = AsyncSearchQueryWrapper(query, max_items=max_items)
        self.page = page if page > 0 else 1
        self.per_page = per_page
        self.max_items = max_items
        self.offset = (self.page - 1) * self.per_page
        self.items = await self.query[self.offset:self.offset + self.per_page]
        self.total = (await self.query.get_result()).total
        return self

    async def prev(self):
        return await self.create(
            self.original_query, **self._prev_page_params()
        )

    async def next(self):
        return await self.create(
            self.original_query, **self._next_page_params()
        )
