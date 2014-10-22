import datetime

import dateutil

from elasticmagic.types import Type, String, Integer, Float, Date, Object, MultiField, List
from elasticmagic.compat import string_types
from elasticmagic.document import Document, DynamicDocument
from elasticmagic.expression import Field

from .base import BaseTestCase


class GroupDocument(Document):
    id = Field(Integer)
    name = Field(String)

class TagDocument(Document):
    id = Field(Integer)
    name = Field(String)

class TestDocument(Document):
    name = Field('test_name', String())
    status = Field(Integer)
    group = Field(Object(GroupDocument))
    price = Field(MultiField(Float, {'sort': Float}))
    tags = Field(List(Object(TagDocument)))
    date_created = Field(Date)
    unused = Field(String)

    __dynamic_fields__ = [
        Field('attr_*', Integer),
    ]


class DocumentTestCase(BaseTestCase):
    def test_document(self):
        self.assertIsInstance(TestDocument._id, Field)
        self.assertIsInstance(TestDocument._id._type, String)
        self.assertIsInstance(TestDocument._score, Field)
        self.assertIsInstance(TestDocument._score._type, Float)
        self.assertIsInstance(TestDocument.name, Field)
        self.assertIsInstance(TestDocument.name._type, String)
        self.assertIsInstance(TestDocument.status, Field)
        self.assertIsInstance(TestDocument.status._type, Integer)
        self.assertIsInstance(TestDocument.group, Field)
        self.assertIsInstance(TestDocument.group._type, Object)
        self.assertIsInstance(TestDocument.group.name, Field)
        self.assertIsInstance(TestDocument.price, Field)
        self.assertIsInstance(TestDocument.price._type, MultiField)
        self.assertIsInstance(TestDocument.price.sort, Field)
        self.assertIsInstance(TestDocument.price.sort._type, Float)
        self.assertEqual(TestDocument.price.sort._collect_doc_classes(), [TestDocument])
        self.assertEqual(TestDocument.group.name._name, 'group.name')
        self.assertIsInstance(TestDocument.group.name._type, String)
        self.assertEqual(TestDocument.group.name._collect_doc_classes(), [TestDocument])
        self.assertRaises(AttributeError, lambda: TestDocument.group.f.missing)
        self.assertIsInstance(TestDocument.attr_2, Field)
        self.assertIsInstance(TestDocument.attr_2._type, Integer)
        self.assertRaises(AttributeError, lambda: TestDocument.fake_attr_1)

        doc = TestDocument()
        self.assertIs(doc._id, None)
        self.assertIs(doc.name, None)
        self.assertIs(doc.status, None)
        doc._id = 123
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)

        doc = TestDocument(_id=123)
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertIs(doc.name, None)
        self.assertIs(doc.status, None)

        doc = TestDocument(_id=123, name='Test name', status=0,
                           group=GroupDocument(name='Test group'),
                           price=99.99,
                           tags=[TagDocument(id=1, name='Test tag'),
                                 TagDocument(id=2, name='Just tag')])
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertIsInstance(doc.name, string_types)
        self.assertEqual(doc.name, 'Test name')
        self.assertIsInstance(doc.status, int)
        self.assertEqual(doc.status, 0)
        self.assertIsInstance(doc.group, GroupDocument)
        self.assertIsInstance(doc.group.name, string_types)
        self.assertIsInstance(doc.price, float)
        self.assertAlmostEqual(doc.price, 99.99)
        self.assertEqual(doc.group.name, 'Test group')
        self.assertIsInstance(doc.tags, list)
        self.assertIsInstance(doc.tags[0].name, string_types)
        self.assertEqual(doc.tags[0].name, 'Test tag')

        hit_doc = TestDocument(
            _hit={
                '_id':'123',
                '_score': 1.23,
                '_source': {
                    'test_name': 'Test name',
                    'status': 0,
                    'group': {'name': 'Test group'},
                    'price': 101.5,
                    'tags': [{'id': 1, 'name': 'Test tag'},
                             {'id': 2, 'name': 'Just tag'}],
                    'date_created': '2014-08-14T14:05:28.789Z',
                }
            }
        )
        self.assertEqual(hit_doc._id, '123')
        self.assertAlmostEqual(hit_doc._score, 1.23)
        self.assertEqual(hit_doc.name, 'Test name')
        self.assertEqual(hit_doc.status, 0)
        self.assertIsInstance(hit_doc.group, GroupDocument)
        self.assertEqual(hit_doc.group.name, 'Test group')
        self.assertAlmostEqual(hit_doc.price, 101.5)
        self.assertIsInstance(hit_doc.tags, list)
        self.assertIsInstance(hit_doc.tags[0], TagDocument)
        self.assertEqual(hit_doc.tags[0].id, 1)
        self.assertEqual(hit_doc.tags[0].name, 'Test tag')
        self.assertEqual(hit_doc.tags[1].id, 2)
        self.assertEqual(hit_doc.tags[1].name, 'Just tag')
        self.assertEqual(hit_doc.date_created,
                         datetime.datetime(2014, 8, 14, 14, 5, 28, 789000, dateutil.tz.tzutc()))
        self.assertIs(hit_doc.unused, None)

        hit_doc = TestDocument(
            _hit={
                '_id':'123'
            }
        )
        self.assertEqual(hit_doc._id, '123')
        self.assertIs(hit_doc.name, None)

        doc = TestDocument(_id=123, name='Test name', status=0,
                           group=GroupDocument(name='Test group'),
                           price=101.5,
                           tags=[TagDocument(id=1, name='Test tag'),
                                 TagDocument(id=2, name='Just tag')],
                           attr_3=45)
        self.assertEqual(
            doc.to_dict(),
            {
                'name': 'Test name',
                'status': 0,
                'group': {
                    'name': 'Test group'
                },
                'price': 101.5,
                'tags': [
                    {'id': 1, 'name': 'Test tag'},
                    {'id': 2, 'name': 'Just tag'},
                ],
                'attr_3': 45
            }
        )
        

    def test_inheritance(self):
        class InheritedDocument(TestDocument):
            description = Field(String)

        doc = InheritedDocument(_id=123)
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertIs(doc.name, None)
        self.assertIs(doc.status, None)
        self.assertIs(doc.description, None)
        self.assertEqual(
            doc.to_dict(),
            {}
        )

        doc = InheritedDocument(_id=123, name='Test', attr_1=1, attr_2=2, face_attr_3=3)
        self.assertEqual(
            doc.to_dict(),
            {
                'name': 'Test',
                'attr_1': 1,
                'attr_2': 2,
            }
        )

    def test_dynamic_document(self):
        self.assertIsInstance(DynamicDocument._id, Field)
        self.assertIsInstance(DynamicDocument._id._type, String)
        self.assertIsInstance(DynamicDocument.name, Field)
        self.assertIsInstance(DynamicDocument.name._type, Type)
        self.assertIsInstance(DynamicDocument.status, Field)
        self.assertIsInstance(DynamicDocument.status._type, Type)
        self.assertIsInstance(DynamicDocument.group, Field)
        self.assertIsInstance(DynamicDocument.group._type, Type)
        self.assertIsInstance(DynamicDocument.group.name, Field)
        self.assertIsInstance(DynamicDocument.group.name._type, Type)
        self.assert_expression(
            DynamicDocument.group.name,
            'group.name'
        )
