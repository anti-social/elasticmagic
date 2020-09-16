import pytest

from .conftest import Car


@pytest.mark.asyncio
async def test_to_dict(es_index, cars):
    sq = (
        es_index.search_query()
        .limit(1)
    )

    assert await sq.to_dict() == {
        'size': 1
    }


@pytest.mark.asyncio
async def test_get_result(es_index, cars):
    sq = (
        es_index.search_query(Car.name.match('Sally'))
        .limit(1)
    )

    res = await sq.get_result()

    assert res.total == 1
    assert len(res.hits) == 1
    assert res.error is None
    doc = res.hits[0]
    assert isinstance(doc, Car)
    assert doc._id == '2'

    cached_res = await sq.get_result()
    assert cached_res is res


@pytest.mark.asyncio
async def test_count(es_index, cars):
    assert await es_index.search_query().count() == 2
    assert await es_index.search_query(Car.name.match('Sally')).count() == 1


@pytest.mark.asyncio
async def test_exists(es_index, cars):
    assert await es_index.search_query().exists()
    assert await es_index.search_query(Car.name.match('Sally')).exists()
    assert not await es_index.search_query(Car.name.match('Buzz')).exists()


@pytest.mark.asyncio
async def test_iter(es_index, cars):
    sq = (
        es_index.search_query(Car.name.match('Sally'))
        .limit(1)
    )

    for doc in await sq:
        assert isinstance(doc, Car)
        assert doc._id == '2'

    for doc in await sq:
        assert isinstance(doc, Car)
        assert doc._id == '2'


@pytest.mark.asyncio
async def test_getitem(es_index, cars):
    sq = es_index.search_query()

    with pytest.raises(TypeError):
        await sq['test']

    with pytest.raises(ValueError):
        await sq[-1]

    with pytest.raises(ValueError):
        await sq[:-1]

    with pytest.raises(ValueError):
        await sq[::2]

    docs = await sq[1:2]
    assert len(docs) == 1

    docs = await sq[:2]
    assert len(docs) == 2

    doc = await sq[0]
    assert doc is not None


@pytest.mark.asyncio
async def test_scroll(es_index, all_cars):
    assert await es_index.search_query().count() == 11

    sq = es_index.search_query(scroll='1m', doc_cls=Car).limit(5)

    res = await sq.get_result()
    assert len(res.hits) == 5

    res = await es_index.scroll(
        scroll_id=res.scroll_id, scroll='1m', doc_cls=Car
    )
    assert len(res.hits) == 5

    res = await es_index.scroll(
        scroll_id=res.scroll_id, scroll='1m', doc_cls=Car
    )
    assert len(res.hits) == 1

    await es_index.clear_scroll(scroll_id=res.scroll_id)
