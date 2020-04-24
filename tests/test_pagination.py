from mock import Mock

import pytest

from elasticmagic.ext.pagination import SearchQueryWrapper
from elasticmagic.ext.pagination.flask import Pagination


def test_pagination(client, index):
    client.search = Mock(
        return_value={
            "hits": {
                "max_score": 1,
                "total": 28,
                "hits": [
                    {
                        "_id": "333",
                        "_type": "car",
                        "_score": 1
                    },
                    {
                        "_id": "444",
                        "_type": "car",
                        "_score": 1
                    }
                ]
            }
        }
    )

    p = Pagination(
        index.search_query(doc_cls=index['car']), page=2, per_page=2
    )

    assert p.total == 28
    assert p.pages == 14
    assert len(p.items) == 2
    assert p.items[0]._id == '333'
    assert p.items[1]._id == '444'
    assert p.has_next is True
    assert p.next_num == 3
    assert p.has_prev is True
    assert p.prev_num == 1
    for page, check_page in zip(
            p.iter_pages(), [1, 2, 3, 4, 5, 6, None, 13, 14]
    ):
        assert page == check_page

    assert client.search.call_count == 1


def test_wrapper(client, index):
    client.search = Mock(
        return_value={
            "hits": {
                "max_score": 1,
                "total": 28,
                "hits": [
                    {
                        "_id": "333",
                        "_type": "car",
                        "_score": 1
                    },
                    {
                        "_id": "444",
                        "_type": "car",
                        "_score": 1
                    }
                ]
            }
        }
    )

    sq = index.search_query(doc_cls=index['car'])
    wrapper = SearchQueryWrapper(sq)
    with pytest.raises(ValueError):
        wrapper[None]
    with pytest.raises(ValueError):
        [d for d in wrapper]
    with pytest.raises(ValueError):
        len(wrapper)
    with pytest.raises(ValueError):
        wrapper.get_result()
    assert len(wrapper[:2]) == 2
    assert len([d for d in wrapper]) == 2
    assert len(wrapper.get_result().hits) == 2
