from elasticmagic.types import String, Integer, Object
from elasticmagic.compat import text_type
from elasticmagic.document import Document
from elasticmagic.expression import Field

from .base import BaseTestCase


class DocumentTestCase(BaseTestCase):
    def test_document(self):
        class GroupDocument(Document):
            name = Field(String)

        class TestDocument(Document):
            name = Field(String())
            status = Field(Integer)
            group = Field(Object(GroupDocument))

        self.assertIsInstance(TestDocument.id, Field)
        self.assertIsInstance(TestDocument.id.type, String)
        self.assertIsInstance(TestDocument.name, Field)

        doc = TestDocument()
        self.assertEqual(doc.id, None)
        self.assertEqual(doc.name, None)
        self.assertEqual(doc.status, None)
        doc.id = 123
        self.assertIsInstance(doc.id, text_type)
        self.assertEqual(doc.id, '123')

        doc = TestDocument(id=123)
        self.assertIsInstance(doc.id, text_type)
        self.assertEqual(doc.id, '123')
        self.assertEqual(doc.name, None)
        self.assertEqual(doc.status, None)

        doc = TestDocument(id=123, name='Test name', status=0,
                           group=GroupDocument(name='Test group'))
        self.assertIsInstance(doc.id, text_type)
        self.assertEqual(doc.id, '123')
        self.assertIsInstance(doc.name, text_type)
        self.assertEqual(doc.name, 'Test name')
        self.assertIsInstance(doc.status, int)
        self.assertEqual(doc.status, 0)
        self.assertIsInstance(doc.group, GroupDocument)
        self.assertIsInstance(doc.group.name, text_type)
        self.assertEqual(doc.group.name, 'Test group')
