import pytest

from .conftest import Car


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
    assert await es_index.search_query(Car.name.match('Sally')).count() == 1


@pytest.mark.asyncio
async def test_exists(es_index, cars):
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

    with pytest.warns(UserWarning, match='Cannot determine document class'):
        docs = await sq[1:2]
        assert len(docs) == 1

    with pytest.warns(UserWarning, match='Cannot determine document class'):
        docs = await sq[:2]
        assert len(docs) == 2

    with pytest.warns(UserWarning, match='Cannot determine document class'):
        doc = await sq[0]
        assert doc is not None
