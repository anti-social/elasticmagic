import unittest

from elasticsearch import Elasticsearch

from elasticmagic import Index, DynamicDocument


class IndexTest(unittest.TestCase):
    def test_index(self):
        es_index = Index(Elasticsearch(), 'test')

        doc_cls = es_index.product
        self.assertEqual(doc_cls.__name__, 'ProductDocument')
        self.assertEqual(doc_cls.__bases__, (DynamicDocument,))

        doc_cls2 = es_index.product
        self.assertIs(doc_cls, doc_cls2)
