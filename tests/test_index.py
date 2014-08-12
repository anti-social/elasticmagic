import unittest

from elasticsearch import Elasticsearch

from elasticmagic import Index, Document


class IndexTest(unittest.TestCase):
    def test_index(self):
        es_index = Index(Elasticsearch(), 'test')

        doc_cls = es_index.product
        self.assertEqual(doc_cls.__name__, 'ProductDocument')
        self.assertEqual(doc_cls.__bases__, (Document,))

        doc_cls2 = es_index.product
        self.assertIs(doc_cls, doc_cls2)
