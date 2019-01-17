from elasticmagic import SearchQuery

import pytest

from ..conftest import Car


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


def test_scroll(es_index, cars):
    with pytest.warns(UserWarning, match='Cannot determine document class'):
        search_res = es_index.search(
            SearchQuery(), scroll='1m',
        )

    assert search_res.total == 2
    assert len(search_res.hits) == 2
    assert search_res.scroll_id is not None

    scroll_res = es_index.scroll(search_res.scroll_id, scroll='1m')

    assert scroll_res.total == 2
    assert len(scroll_res.hits) == 0

    clear_scroll_res = es_index.clear_scroll(scroll_res.scroll_id)

    assert clear_scroll_res.succeeded is True
