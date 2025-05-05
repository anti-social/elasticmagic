import datetime
import pytest

from elasticmagic import Document, DynamicDocument, Field, Script
from elasticmagic import actions
from elasticmagic.compiler import Compiler_6_0
from elasticmagic.compiler import Compiler_7_0
from elasticmagic.types import Date
from elasticmagic.types import Integer
from elasticmagic.types import List
from elasticmagic.types import Text


class OrderDocument(Document):
    __doc_type__ = 'order'

    product_ids = Field(List(Integer))
    date_created = Field(Date)


class ProductWithoudTypeDocument(Document):
    name = Field(Text)


@pytest.fixture(
    params=[
        Compiler_6_0,
        Compiler_7_0,
    ]
)
def compiler(request):
    yield request.param

@pytest.fixture
def order_doc():
    yield OrderDocument(
        product_ids=[1, 2, 3],
        date_created=datetime.datetime(2019, 1, 1)
    )


def test_index_action_dict(compiler):
    action = actions.Index(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        refresh=True
    )
    if compiler.features.requires_doc_type:
        expected_meta = {
            'index': {
                '_id': 1,
                '_type': 'test',
                'refresh': True,
            }
        }
    else:
        expected_meta = {
            'index': {
                '_id': 1,
                'refresh': True,
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) == {
        'name': 'Test',
    }


def test_index_action_document(compiler, order_doc):
    action = actions.Index(order_doc, index='orders-2019')
    if compiler.features.requires_doc_type:
        expected_meta = {
            'index': {
                '_type': 'order',
                '_index': 'orders-2019',
            }
        }
    else:
        expected_meta = {
            'index': {
                '_index': 'orders-2019',
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) == {
        'product_ids': [1, 2, 3],
        'date_created': datetime.datetime(2019, 1, 1),
    }


def test_index_action_document_withoud_doc_type(compiler):
    doc = ProductWithoudTypeDocument(name='Type is unknown')
    action = actions.Index(doc, index='any', doc_type='product')
    assert action.to_meta(compiler=compiler) == {
        'index': {
            '_type': 'product',
            '_index': 'any',
        }
    }
    assert action.to_source(compiler=compiler) == {
        'name': 'Type is unknown',
    }


def test_delete_action_dict(compiler):
    action = actions.Delete(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        routing=2
    )
    if compiler.features.requires_doc_type:
        expected_meta = {
            'delete': {
                '_id': 1,
                '_type': 'test',
                'routing': 2,
            }
        }
    else:
        expected_meta = {
            'delete': {
                '_id': 1,
                'routing': 2,
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) is None


def test_delete_action_document(compiler, order_doc):
    action = actions.Delete(order_doc, index='orders-2019')
    if compiler.features.requires_doc_type:
        expected_meta = {
            'delete': {
                '_type': 'order',
                '_index': 'orders-2019',
            }
        }
    else:
        expected_meta = {
            'delete': {
                '_index': 'orders-2019',
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) is None


def test_delete_action_dynamic_document(compiler):
    action = actions.Delete(
        DynamicDocument(_id='1', _type='order', _index='orders-2019'),
        index='orders-2022'
    )
    if compiler.features.requires_doc_type:
        expected_meta = {
            'delete': {
                '_id': '1',
                '_type': 'order',
                '_index': 'orders-2022',
            }
        }
    else:
        expected_meta = {
            'delete': {
                '_id': '1',
                '_index': 'orders-2022',
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) is None


def test_create_action_dict(compiler):
    action = actions.Create(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        refresh=True
    )
    if compiler.features.requires_doc_type:
        expected_meta = {
            'create': {
                '_id': 1,
                '_type': 'test',
                'refresh': True,
            }
        }
    else:
        expected_meta = {
            'create': {
                '_id': 1,
                'refresh': True,
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) == {
        'name': 'Test',
    }


def test_create_action_document(compiler, order_doc):
    action = actions.Create(order_doc, index='orders-2019')
    if compiler.features.requires_doc_type:
        expected_meta = {
            'create': {
                '_type': 'order',
                '_index': 'orders-2019',
            }
        }
    else:
        expected_meta = {
            'create': {
                '_index': 'orders-2019',
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) == {
        'product_ids': [1, 2, 3],
        'date_created': datetime.datetime(2019, 1, 1),
    }


def test_update_action_dict(compiler):
    action = actions.Update(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        refresh=True
    )
    if compiler.features.requires_doc_type:
        expected_meta = {
            'update': {
                '_id': 1,
                '_type': 'test',
                'refresh': True,
            }
        }
    else:
        expected_meta = {
            'update': {
                '_id': 1,
                'refresh': True,
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta
    assert action.to_source(compiler=compiler) == {
        'doc': {
            'name': 'Test',
        }
    }


@pytest.mark.parametrize('compiler', [Compiler_6_0, Compiler_7_0])
def test_update_action_script(compiler):
    action = actions.Update(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        script=Script(inline='ctx._source.product_ids.append(911)'),
        upsert={'name': 'Test via upsert'},
        refresh=True
    )
    if compiler.features.requires_doc_type:
        expected_meta = {
            'update': {
                '_id': 1,
                '_type': 'test',
                'refresh': True,
            }
        }
    else:
        expected_meta = {
            'update': {
                '_id': 1,
                'refresh': True,
            }
        }
    assert action.to_meta(compiler=compiler) == expected_meta

    assert action.to_source(compiler=compiler) == {
        'script': {
            'source': 'ctx._source.product_ids.append(911)',
        },
        'upsert': {
            'name': 'Test via upsert',
        },
    }
