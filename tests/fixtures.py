import pytest

from elasticsearch import Elasticsearch

from elasticmagic import Cluster, Index


@pytest.fixture
def client():
    yield Elasticsearch()


@pytest.fixture
def cluster(client):
    yield Cluster(client)


@pytest.fixture
def index(cluster):
    yield Index(cluster, 'test')
