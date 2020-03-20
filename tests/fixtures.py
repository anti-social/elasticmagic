import pytest

from elasticsearch import Elasticsearch

from elasticmagic import Cluster, Index
from elasticmagic.compiler import Compiler_1_0
from elasticmagic.compiler import Compiler_2_0
from elasticmagic.compiler import Compiler_5_0
from elasticmagic.compiler import Compiler_6_0
from elasticmagic.compiler import Compiler_7_0


@pytest.fixture
def client():
    yield Elasticsearch()


@pytest.fixture
def cluster(client):
    yield Cluster(
        client, autodetect_es_version=False, compiler=Compiler_5_0
    )


@pytest.fixture
def index(cluster):
    yield Index(cluster, 'test')


@pytest.fixture(
    params=[
        Compiler_1_0, Compiler_2_0, Compiler_5_0, Compiler_6_0, Compiler_7_0,
    ]
)
def compiler(request):
    return request.param
