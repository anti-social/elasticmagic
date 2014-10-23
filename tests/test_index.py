import unittest

from mock import MagicMock

from elasticmagic import Index, Document, DynamicDocument, Field
from elasticmagic.types import String


class IndexTest(unittest.TestCase):
    def setUp(self):
        self.es_client = MagicMock()
        self.es_index = Index(self.es_client, 'test')

    def test_auto_doc_cls(self):
        doc_cls = self.es_index.product
        self.assertEqual(doc_cls.__name__, 'ProductDocument')
        self.assertEqual(doc_cls.__bases__, (DynamicDocument,))

        self.assertIs(doc_cls, self.es_index.product)

    def test_add(self):
        class CarDocument(Document):
            __doc_type__ = 'car'

            vendor = Field(String)
            model = Field(String)

        doc = CarDocument(_id='test_id', vendor='Subaru', model='VRX')
        doc._routing = 'Subaru'
        self.es_index.add([doc])
        self.es_client.bulk.assert_called_with(
            index='test',
            body=[
                {'index': {'_type': 'car', '_id': 'test_id', '_routing': 'Subaru'}},
                {'vendor': 'Subaru', 'model': 'VRX'}
            ],
        )
