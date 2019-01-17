import os

import pytest

from elasticmagic import Cluster

from elasticsearch import Elasticsearch

from ..conftest import Car


@pytest.fixture
def es_client():
    es_url = os.environ.get('ES_URL', 'localhost:9200')
    es_client = Elasticsearch([es_url])
    yield es_client
    if hasattr(es_client.transport, 'close'):
        es_client.transport.close()


@pytest.fixture
def es_cluster(es_client):
    return Cluster(es_client)


@pytest.fixture
def es_index(es_cluster, es_client, index_name):
    es_client.indices.create(index=index_name)
    es_index = es_cluster[index_name]
    es_index.put_mapping(Car)
    yield es_index
    es_client.indices.delete(index=index_name)


@pytest.fixture
def cars(es_index, car_docs):
    es_index.add(car_docs, refresh=True)
    yield car_docs
