import asyncio
import gc
import os
import warnings

from elasticsearch import AsyncElasticsearch

import pytest
import pytest_asyncio

from elasticmagic.ext.asyncio.cluster import AsyncCluster

from ..conftest import Car


@pytest.fixture
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


@pytest_asyncio.fixture
async def es_client(event_loop, es_url):
    es_url = os.environ.get('ES_URL', es_url)
    es_client = AsyncElasticsearch([es_url], event_loop=event_loop)
    yield es_client
    await es_client.transport.close()


@pytest_asyncio.fixture
def es_cluster(es_client):
    yield AsyncCluster(es_client)


@pytest_asyncio.fixture
async def es_index(es_cluster, es_client, index_name):
    await es_client.indices.create(index=index_name)
    es_index = es_cluster[index_name]
    await es_index.put_mapping(Car)
    yield es_index
    await es_client.indices.delete(index=index_name)


@pytest_asyncio.fixture
async def cars(es_index, car_docs):
    await es_index.add(car_docs, refresh=True)
    yield car_docs


@pytest_asyncio.fixture
async def all_cars(es_index, all_car_docs):
    await es_index.add(all_car_docs, refresh=True)
    yield all_car_docs
