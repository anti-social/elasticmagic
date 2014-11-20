from mock import MagicMock

from elasticmagic import Index, Document, DynamicDocument, MatchAll, Field
from elasticmagic.types import String

from .base import BaseTestCase


class IndexTest(BaseTestCase):
    def test_auto_doc_cls(self):
        doc_cls = self.index.product
        self.assertEqual(doc_cls.__name__, 'ProductDocument')
        self.assertEqual(doc_cls.__bases__, (DynamicDocument,))

        self.assertIs(doc_cls, self.index.product)

    def test_count(self):
        self.index.count(MatchAll(), 'car', routing=123)
        self.client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match_all': {}
                }
            },
            routing=123
        )

        self.index.count(self.index.car.name.match('Subaru'), 'car')
        self.client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match': {
                        'name': 'Subaru'
                    }
                }
            }
        )

    def test_exists(self):
        self.index.exists(MatchAll(), 'car', refresh=True)
        self.client.exists.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match_all': {}
                }
            },
            refresh=True
        )

        self.index.exists(self.index.car.name.match('Subaru'), 'car')
        self.client.exists.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match': {
                        'name': 'Subaru'
                    }
                }
            }
        )

    def test_add(self):
        class CarDocument(Document):
            __doc_type__ = 'car'

            vendor = Field(String)
            model = Field(String)

        doc = CarDocument(_id='test_id', vendor='Subaru', model='VRX')
        doc._routing = 'Subaru'
        self.index.add([doc])
        self.client.bulk.assert_called_with(
            index='test',
            body=[
                {'index': {'_type': 'car', '_id': 'test_id', '_routing': 'Subaru'}},
                {'vendor': 'Subaru', 'model': 'VRX'}
            ],
        )

    def test_add_raw_docs(self):
        docs = [
            {
                '_id': '1',
                '_routing': 'Subaru',
                '_type': 'car',
                'vendor': 'Subaru',
                'model': 'VRX'
            },
            {
                '_id': '2',
                '_routing': 'Nissan',
                '_type': 'car',
                'vendor': 'Nissan',
                'model': 'X-Trail'
            },
        ]
        self.index.add(docs)
        self.client.bulk.assert_called_with(
            index='test',
            body=[
                {'index': {'_type': 'car', '_id': '1', '_routing': 'Subaru'}},
                {'vendor': 'Subaru', 'model': 'VRX'},
                {'index': {'_type': 'car', '_id': '2', '_routing': 'Nissan'}},
                {'vendor': 'Nissan', 'model': 'X-Trail'}
            ],
        )
        
