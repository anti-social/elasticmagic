import uuid

from elasticsearch import Elasticsearch

import pytest

from elasticmagic import Cluster, Document, Field
from elasticmagic.types import String


class Car(Document):
    __doc_type__ = 'car'

    name = Field(String())


@pytest.fixture
def es_client():
    es_client = Elasticsearch(['localhost:9200'])
    yield es_client
    es_client.transport.close()


@pytest.fixture
def es_cluster(es_client):
    yield Cluster(es_client)


@pytest.fixture
def es_index(es_cluster, es_client):
    index_name = 'test-{}'.format(str(uuid.uuid4()).split('-')[0])
    es_client.indices.create(index=index_name)
    yield es_cluster[index_name]
    es_client.indices.delete(index=index_name)


def test_adding_documents(es_index):
    es_index.add(
        [
            Car(_id=1, name='Lightning McQueen'),
            Car(_id=2, name='Sally Carerra'),
        ]
    )

    doc = es_index.get(1, doc_cls=Car)
    assert doc.name == 'Lightning McQueen'
    assert doc._id == '1'
    assert doc._index == es_index.get_name()
    assert doc._score is None

    doc = es_index.get(2, doc_cls=Car)
    assert doc.name == 'Sally Carerra'
    assert doc._id == '2'
    assert doc._index == es_index.get_name()
    assert doc._score is None
