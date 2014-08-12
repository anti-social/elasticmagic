from elasticmagic.types import Type, String, Integer, Object
from elasticmagic.compat import string_types
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

        self.assertIsInstance(TestDocument._id, Field)
        self.assertIsInstance(TestDocument._id._type, String)
        self.assertIsInstance(TestDocument.name, Field)
        self.assertIsInstance(TestDocument.name._type, String)
        self.assertIsInstance(TestDocument.status, Field)
        self.assertIsInstance(TestDocument.status._type, Integer)
        self.assertIsInstance(TestDocument.group, Field)
        self.assertIsInstance(TestDocument.group._type, Object)
        self.assertIsInstance(TestDocument.group.f.name, Field)
        self.assertIsInstance(TestDocument.group.f.name._type, String)
        self.assertIsInstance(TestDocument.group.f.missing, Field)
        self.assertIsInstance(TestDocument.group.f.missing._type, Type)

        doc = TestDocument()
        self.assertEqual(doc._id, None)
        self.assertEqual(doc.name, None)
        self.assertEqual(doc.status, None)
        doc._id = 123
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)

        doc = TestDocument(_id=123)
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertEqual(doc.name, None)
        self.assertEqual(doc.status, None)

        doc = TestDocument(_id=123, name='Test name', status=0,
                           group=GroupDocument(name='Test group'))
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertIsInstance(doc.name, string_types)
        self.assertEqual(doc.name, 'Test name')
        self.assertIsInstance(doc.status, int)
        self.assertEqual(doc.status, 0)
        self.assertIsInstance(doc.group, GroupDocument)
        self.assertIsInstance(doc.group.name, string_types)
        self.assertEqual(doc.group.name, 'Test group')


# class ProductCompanyDoc(Document):
#     id = Field(Integer)


# class ProductDoc(Document):
#     __doc_type__ = 'product'

#     name = Field(String)
#     keywords = Field(String)
#     name_keywords = Field(String)
#     description = Field(String)

#     company = Field(Object(ProductCompanyDoc))
