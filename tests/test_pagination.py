from unittest.mock import Mock

from elasticmagic.ext.pagination import SearchQueryWrapper
from elasticmagic.ext.pagination.flask import Pagination

from .base import BaseTestCase


class FlaskPaginationTest(BaseTestCase):
    def test_pagination(self):
        self.client.search = Mock(
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

        p = Pagination(self.index.search_query(doc_cls=self.index['car']), page=2, per_page=2)

        self.assertEqual(p.total, 28)
        self.assertEqual(p.pages, 14)
        self.assertEqual(len(p.items), 2)
        self.assertEqual(p.items[0]._id, '333')
        self.assertEqual(p.items[1]._id, '444')
        self.assertEqual(p.has_next, True)
        self.assertEqual(p.next_num, 3)
        self.assertEqual(p.has_prev, True)
        self.assertEqual(p.prev_num, 1)
        for page, check_page in zip(p.iter_pages(), [1, 2, 3, 4, 5, 6, None, 13, 14]):
            self.assertEqual(page, check_page)

        self.assertEqual(self.client.search.call_count, 1)

    def test_wrapper(self):
        self.client.search = Mock(
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

        sq = self.index.search_query(doc_cls=self.index['car'])
        wrapper = SearchQueryWrapper(sq)
        self.assertRaises(ValueError, lambda: wrapper[None])
        self.assertRaises(ValueError, lambda: [d for d in wrapper])
        self.assertRaises(ValueError, lambda: len(wrapper))
        self.assertRaises(ValueError, lambda: wrapper.get_result())
        self.assertEqual(len(wrapper[:2]), 2)
        self.assertEqual(len([d for d in wrapper]), 2)
        self.assertEqual(len(wrapper.get_result().hits), 2)
