import asyncio
import gc
import os
import uuid
import warnings

from elasticsearch_async import AsyncElasticsearch

import pytest

from elasticmagic import Document, Field
from elasticmagic.ext.asyncio.cluster import AsyncCluster
from elasticmagic.types import Text


class Car(Document):
    __doc_type__ = 'car'

    name = Field(Text())


@pytest.yield_fixture
def event_loop(request):
    """Create an instance of the default event loop for each test case.
    Also catches all warnings and raises exception if there was
    'coroutine was never awaited' wargning.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()

    with warnings.catch_warnings(record=True) as catched_warnings:
        yield loop
        # Collecting garbage should trigger warning for non-awaited coroutines
        gc.collect()

    for w in catched_warnings:
        if (
                isinstance(w.message, RuntimeWarning) and
                str(w.message).endswith('was never awaited')
        ):
            raise w.message
        else:
            warnings.showwarning(w.message, w.category, w.filename, w.lineno)

    loop.close()


@pytest.fixture
async def es_client(event_loop):
    es_url = os.environ.get('ES_URL', 'localhost:9200')
    es_client = AsyncElasticsearch([es_url], event_loop=event_loop)
    yield es_client
    await es_client.transport.close()


@pytest.fixture
def es_cluster(es_client):
    yield AsyncCluster(es_client)


@pytest.fixture
async def es_index(es_cluster, es_client):
    index_name = 'test-{}'.format(str(uuid.uuid4()).split('-')[0])
    await es_client.indices.create(index=index_name)
    es_index = es_cluster[index_name]
    await es_index.put_mapping(Car)
    yield es_index
    await es_client.indices.delete(index=index_name)


@pytest.fixture
async def cars(es_index):
    cars = [
        Car(_id=1, name='Lightning McQueen'),
        Car(_id=2, name='Sally Carerra'),
    ]
    await es_index.add(cars, refresh=True)
    yield cars


@pytest.fixture
async def all_cars(es_index):
    cars = [
        Car(_id=1, name='Lightning McQueen'),
        Car(_id=2, name='Sally Carerra'),
        Car(_id=3, name='Doc Hudson'),
        Car(_id=4, name='Ramone'),
        Car(_id=5, name='Luigi'),
        Car(_id=6, name='Guido'),
        Car(_id=7, name='Flo'),
        Car(_id=8, name='Sarge'),
        Car(_id=9, name='Sheriff'),
        Car(_id=10, name='Fillmore'),
        Car(_id=11, name='Mack'),
    ]
    await es_index.add(cars, refresh=True)
    yield cars
