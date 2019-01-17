import pytest

from .conftest import Car

from elasticmagic.ext.asyncio.pagination import AsyncSearchQueryWrapper
from elasticmagic.ext.asyncio.pagination.flask import AsyncPagination


@pytest.mark.asyncio
async def test_search_query_wrapper(es_index, all_cars):
    sq = (
        es_index.search_query(doc_cls=Car)
    )

    wrapped_sq = AsyncSearchQueryWrapper(sq)

    with pytest.raises(ValueError):
        await wrapped_sq[0]

    with pytest.raises(ValueError):
        await wrapped_sq

    with pytest.raises(ValueError):
        len(wrapped_sq)

    with pytest.raises(ValueError):
        await wrapped_sq.get_result()

    hits = await wrapped_sq[:3]
    assert len(hits) == 3

    hits = await wrapped_sq[5:]
    assert len(hits) == 6

    assert len(wrapped_sq) == 11

    hits = await wrapped_sq
    assert len(list(hits)) == 6


@pytest.mark.asyncio
async def test_flask_pagination_default(es_index, all_cars):
    sq = (
        es_index.search_query(doc_cls=Car)
    )
    p = await AsyncPagination.create(sq)

    assert len(p.items) == 10
    assert p.page == 1
    assert p.pages == 2
    assert p.total == 11
    assert p.has_prev is False
    assert p.prev_num is None
    assert p.has_next is True
    assert p.next_num == 2
    assert list(p.iter_pages()) == [1, 2]

    p2 = await p.next()
    assert len(p2.items) == 1
    assert p2.page == 2
    assert p2.pages == 2
    assert p2.total == 11
    assert p2.has_prev is True
    assert p2.prev_num == 1
    assert p2.has_next is False
    assert p2.next_num is None
    assert list(p2.iter_pages()) == [1, 2]

    p1 = await p2.prev()
    assert p1.page == 1


@pytest.mark.asyncio
async def test_flask_pagination_many_pages(es_index, all_cars):
    sq = (
        es_index.search_query(doc_cls=Car)
    )
    p = await AsyncPagination.create(sq, per_page=2)

    assert len(p.items) == 2
    assert p.page == 1
    assert p.pages == 6
    assert p.total == 11
    assert p.has_prev is False
    assert p.prev_num is None
    assert p.has_next is True
    assert p.next_num == 2
    assert list(p.iter_pages(right_current=2)) == \
        [1, 2, None, 5, 6]


@pytest.mark.asyncio
async def test_flask_pagination_max_items(es_index, all_cars):
    sq = (
        es_index.search_query(doc_cls=Car)
    )
    p = await AsyncPagination.create(sq, page=3, per_page=5, max_items=10)

    assert len(p.items) == 0
    assert p.page == 3
    assert p.has_next is False
    assert p.has_prev is True
    assert list(p.iter_pages()) == [1, 2]
