import uuid

from elasticsearch_async import AsyncElasticsearch

import pytest

from elasticmagic import Document, Field
from elasticmagic.ext.asyncio.cluster import AsyncCluster
from elasticmagic.types import String


class Car(Document):
    __doc_type__ = 'car'

    name = Field(String())


@pytest.fixture
def es_client(event_loop):
    yield AsyncElasticsearch(['localhost:9200'], event_loop=event_loop)


@pytest.fixture
def es_cluster(es_client):
    yield AsyncCluster(es_client)


@pytest.fixture
async def es_index(es_cluster, es_client):
    index_name = 'test-{}'.format(str(uuid.uuid4()).split('-')[0])
    await es_client.indices.create(index=index_name)
    yield es_cluster[index_name]
    await es_client.indices.delete(index=index_name)


@pytest.mark.asyncio
async def test_adding_documents(es_index):
    await es_index.add(
        [
            Car(_id=1, name='Lightning McQueen'),
            Car(_id=2, name='Sally Carerra'),
        ]
    )

    doc = await es_index.get(1, doc_cls=Car)
    assert doc.name == 'Lightning McQueen'
    assert doc._id == '1'
    assert doc._index == es_index.get_name()
    assert doc._score is None

    doc = await es_index.get(2, doc_cls=Car)
    assert doc.name == 'Sally Carerra'
    assert doc._id == '2'
    assert doc._index == es_index.get_name()
    assert doc._score is None
