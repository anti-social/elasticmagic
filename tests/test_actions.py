import datetime
import pytest

from elasticmagic import Document, Field, Script
from elasticmagic import actions
from elasticmagic.compiler import Compiler_1_0
from elasticmagic.compiler import Compiler_2_0
from elasticmagic.compiler import Compiler_5_0
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
        Compiler_1_0,
        Compiler_2_0,
        Compiler_5_0,
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
    assert action.to_meta(compiler=compiler) == {
        'index': {
            '_id': 1,
            '_type': 'test',
            'refresh': True,
        }
    }
    assert action.to_source(compiler=compiler) == {
        'name': 'Test',
    }


def test_index_action_document(compiler, order_doc):
    action = actions.Index(order_doc, index='orders-2019')
    assert action.to_meta(compiler=compiler) == {
        'index': {
            '_type': 'order',
            '_index': 'orders-2019',
        }
    }
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
    assert action.to_meta(compiler=compiler) == {
        'delete': {
            '_id': 1,
            '_type': 'test',
            '_routing': 2,
        }
    }
    assert action.to_source(compiler=compiler) is None


def test_delete_action_document(compiler, order_doc):
    action = actions.Delete(order_doc, index='orders-2019')
    assert action.to_meta(compiler=compiler) == {
        'delete': {
            '_type': 'order',
            '_index': 'orders-2019',
        }
    }
    assert action.to_source(compiler=compiler) is None


def test_create_action_dict(compiler):
    action = actions.Create(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        refresh=True
    )
    assert action.to_meta(compiler=compiler) == {
        'create': {
            '_id': 1,
            '_type': 'test',
            'refresh': True,
        }
    }
    assert action.to_source(compiler=compiler) == {
        'name': 'Test',
    }


def test_create_action_document(compiler, order_doc):
    action = actions.Create(order_doc, index='orders-2019')
    assert action.to_meta(compiler=compiler) == {
        'create': {
            '_type': 'order',
            '_index': 'orders-2019',
        }
    }
    assert action.to_source(compiler=compiler) == {
        'product_ids': [1, 2, 3],
        'date_created': datetime.datetime(2019, 1, 1),
    }


def test_update_action_dict(compiler):
    action = actions.Update(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        refresh=True
    )
    assert action.to_meta(compiler=compiler) == {
        'update': {
            '_id': 1,
            '_type': 'test',
            'refresh': True,
        }
    }
    assert action.to_source(compiler=compiler) == {
        'doc': {
            'name': 'Test',
        }
    }


@pytest.mark.parametrize('compiler', [Compiler_2_0, Compiler_5_0])
def test_update_action_script(compiler):
    action = actions.Update(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        script=Script(inline='ctx._source.product_ids.append(911)'),
        upsert={'name': 'Test via upsert'},
        refresh=True
    )
    assert action.to_meta(compiler=compiler) == {
        'update': {
            '_id': 1,
            '_type': 'test',
            'refresh': True,
        }
    }
    assert action.to_source(compiler=compiler) == {
        'script': {
            'inline': 'ctx._source.product_ids.append(911)',
        },
        'upsert': {
            'name': 'Test via upsert',
        },
    }


def test_update_action_script_compiler_1_0():
    compiler = Compiler_1_0
    action = actions.Update(
        {'_id': 1, '_type': 'test', 'name': 'Test'},
        script=Script(
            inline='ctx._source.product_ids.append(911)',
            params={'product_id': 911}
        ),
        upsert={'name': 'Test via upsert'},
        refresh=True
    )
    assert action.to_meta(compiler=compiler) == {
        'update': {
            '_id': 1,
            '_type': 'test',
            'refresh': True,
        }
    }
    # TODO: For Elasticsearch 1.x we should put script in-place
    # assert action.to_source(compiler=compiler) == {
    #     'script': 'ctx._source.product_ids.append(product_id)',
    #     'params': {
    #         'product_id': 911
    #     },
    #     'upsert': {
    #         'name': 'Test via upsert',
    #     },
    # }
