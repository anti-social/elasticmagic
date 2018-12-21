import asyncio
import gc
import uuid
import warnings

from elasticsearch_async import AsyncElasticsearch

import pytest

from elasticmagic import Document, Field
from elasticmagic.ext.asyncio.cluster import AsyncCluster
from elasticmagic.types import String


class Car(Document):
    __doc_type__ = 'car'

    name = Field(String())


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
    es_client = AsyncElasticsearch(['localhost:9200'], event_loop=event_loop)
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
async def docs(es_index):
    docs = [
        Car(_id=1, name='Lightning McQueen'),
        Car(_id=2, name='Sally Carerra'),
    ]
    await es_index.add(docs, refresh=True)
    yield docs
