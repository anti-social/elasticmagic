import pytest

from elasticsearch import Elasticsearch

from elasticmagic import Cluster, Index
from elasticmagic.compiler import all_compilers
from elasticmagic.compiler import Compiler_7_0


@pytest.fixture
def client():
    yield Elasticsearch()


@pytest.fixture
def cluster(client):
    yield Cluster(
        client, autodetect_es_version=False, compiler=Compiler_7_0
    )


@pytest.fixture
def index(cluster):
    yield Index(cluster, 'test')


@pytest.fixture(params=all_compilers)
def compiler(request):
    return request.param
