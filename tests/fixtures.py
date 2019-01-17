import pytest

from elasticsearch import Elasticsearch

from elasticmagic import Cluster, Index
from elasticmagic.compiler import DefaultCompiler


@pytest.fixture
def client():
    yield Elasticsearch()


@pytest.fixture
def cluster(client):
    yield Cluster(
        client, autodetect_es_version=False, compiler=DefaultCompiler
    )


@pytest.fixture
def index(cluster):
    yield Index(cluster, 'test')
