import uuid

import pytest

from elasticmagic import Document, Field
from elasticmagic.types import Text


class Car(Document):
    __doc_type__ = 'car'

    name = Field(Text())


@pytest.fixture
def index_name():
    return 'test-{}'.format(str(uuid.uuid4()).split('-')[0])


@pytest.fixture
def car_docs():
    yield [
        Car(_id=1, name='Lightning McQueen'),
        Car(_id=2, name='Sally Carerra'),
    ]


@pytest.fixture
def all_car_docs():
    yield [
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
