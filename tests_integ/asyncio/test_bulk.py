import pytest

from elasticmagic.actions import Create
from elasticmagic.actions import Delete
from elasticmagic.actions import Index
from elasticmagic.actions import Update

from .conftest import Car


@pytest.mark.asyncio
async def test_doc_params(es_index):
    res = await es_index.bulk(
        [
            Index(Car(_id=1, _routing=2, name='Doc Hudson'))
        ],
        refresh=True
    )
    assert not res.errors
    assert res.items[0].status == 201

    doc = await es_index.get(1, doc_cls=Car, routing=2)
    assert doc._id == '1'
    assert doc._routing == '2'
    assert doc._version == 1
    assert doc.name == 'Doc Hudson'

    await es_index.bulk(
        [
            Delete(Car(_id=1, _routing=2))
        ],
        refresh=True,
    )
    assert (await es_index.count()).count == 0


@pytest.mark.asyncio
async def test_bulk_update_params(es_index):
    res = await es_index.bulk(
        [
            Update(
                Car(_id=1, name='Doc Hudson'),
                doc_as_upsert=True,
                retry_on_conflict=2,
            )
        ],
        refresh=True
    )
    assert not res.errors
    assert res.items[0].status == 201

    doc = await es_index.get(1, doc_cls=Car)
    assert doc._id == '1'
    assert doc._routing is None
    assert doc._version == 1
    assert doc.name == 'Doc Hudson'


@pytest.mark.asyncio
async def test_bulk_create_params(es_index):
    res = await es_index.bulk(
        [
            Create(Car(_id=1, _routing=2, name='Doc Hudson'))
        ],
        refresh=True
    )
    assert not res.errors
    assert res.items[0].status == 201

    doc = await es_index.get(Car(_id=1, _routing=2))
    assert doc._id == '1'
    assert doc._routing == '2'
    assert doc._version == 1
    assert doc.name == 'Doc Hudson'
