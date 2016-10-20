import datetime

import dateutil

from elasticmagic.util import collect_doc_classes
from elasticmagic.types import (
    Type, String, Integer, Float, Boolean,
    Date, Object, List, GeoPoint, Completion,
)
from elasticmagic.types import ValidationError
from elasticmagic.compat import string_types
from elasticmagic.document import Document, DynamicDocument
from elasticmagic.attribute import AttributedField, DynamicAttributedField
from elasticmagic.expression import Field

from .base import BaseTestCase


class GroupDocument(Document):
    id = Field(Integer)
    name = Field('test_name', String, fields={'raw': Field(String)})

    __dynamic_fields__ = [
        Field('group_id_*', Integer),
    ]


class TagDocument(Document):
    id = Field(Integer)
    name = Field(String)
    group = Field(Object(GroupDocument))


class TestDocument(Document):
    _all = Field(enable=False)

    name = Field('test_name', String(), fields={'raw': Field(String)})
    status = Field(Integer)
    group = Field(Object(GroupDocument))
    price = Field(Float)
    tags = Field(List(Object(TagDocument)))
    date_created = Field(Date)
    unused = Field(String)

    __dynamic_fields__ = [
        Field('i_attr_*', Integer),
        Field('b_attr_*', Boolean),
    ]


class DocumentTestCase(BaseTestCase):
    # def test(self):
    #     class TestDocument(Document):
    #         _all = Field(enable=False)
    #     1/0

    def assertSameElements(self, expected_seq, actual_seq):
        first_seq = list(expected_seq)
        second_seq = list(actual_seq)
        self.assertEqual(len(first_seq), len(second_seq))
        for e1, e2 in zip(first_seq, second_seq):
            self.assertIs(e1, e2)

    def test_base_document(self):
        self.assertSameElements(
            list(Document.fields),
            list(Document.mapping_fields)
        )
        self.assertSameElements(
            list(Document.fields),
            [
                Document._uid,
                Document._id,
                Document._type,
                Document._source,
                Document._all,
                Document._analyzer,
                Document._boost,
                Document._parent,
                Document._field_names,
                Document._routing,
                Document._index,
                Document._size,
                Document._timestamp,
                Document._ttl,
                Document._version,
                Document._score,
            ]
        )

    def test_document(self):
        self.assertSameElements(
            list(TestDocument.fields),
            [
                TestDocument._uid,
                TestDocument._id,
                TestDocument._type,
                TestDocument._source,
                TestDocument._analyzer,
                TestDocument._boost,
                TestDocument._parent,
                TestDocument._field_names,
                TestDocument._routing,
                TestDocument._index,
                TestDocument._size,
                TestDocument._timestamp,
                TestDocument._ttl,
                TestDocument._version,
                TestDocument._score,
                TestDocument._all,
                TestDocument.name,
                TestDocument.status,
                TestDocument.group,
                TestDocument.price,
                TestDocument.tags,
                TestDocument.date_created,
                TestDocument.unused,
            ]
        )
        self.assertSameElements(
            list(TestDocument.user_fields),
            [
                TestDocument.name,
                TestDocument.status,
                TestDocument.group,
                TestDocument.price,
                TestDocument.tags,
                TestDocument.date_created,
                TestDocument.unused,
            ]
        )
        self.assertSameElements(
            list(TestDocument.mapping_fields),
            [
                TestDocument._uid,
                TestDocument._id,
                TestDocument._type,
                TestDocument._source,
                TestDocument._analyzer,
                TestDocument._boost,
                TestDocument._parent,
                TestDocument._field_names,
                TestDocument._routing,
                TestDocument._index,
                TestDocument._size,
                TestDocument._timestamp,
                TestDocument._ttl,
                TestDocument._version,
                TestDocument._score,
                TestDocument._all,
            ]
        )
        self.assertIsInstance(TestDocument._id, AttributedField)
        self.assertIsInstance(TestDocument._id.get_field().get_type(), String)
        self.assertEqual(TestDocument._id.get_field().get_name(), '_id')
        self.assertEqual(TestDocument._id.get_attr_name(), '_id')
        self.assertIs(TestDocument._id.get_parent(), TestDocument)
        self.assert_expression(TestDocument._id, '_id')

        self.assertIsInstance(TestDocument._all, AttributedField)
        self.assertIsInstance(TestDocument._all.get_field().get_type(), String)
        self.assertEqual(TestDocument._all.get_field().get_name(), '_all')
        self.assertEqual(TestDocument._all.get_attr_name(), '_all')
        self.assertIs(TestDocument._all.get_parent(), TestDocument)
        self.assert_expression(TestDocument._all, '_all')

        self.assertIsInstance(TestDocument._score, AttributedField)
        self.assertIsInstance(TestDocument._score.get_field().get_type(), Float)
        self.assertEqual(TestDocument._score.get_field().get_name(), '_score')
        self.assertEqual(TestDocument._score.get_attr_name(), '_score')
        self.assertIs(TestDocument._score.get_parent(), TestDocument)
        self.assert_expression(TestDocument._score, '_score')

        self.assertIsInstance(TestDocument.name, AttributedField)
        self.assertIsInstance(TestDocument.name.get_field().get_type(), String)
        self.assertEqual(TestDocument.name.get_field().get_name(), 'test_name')
        self.assertEqual(TestDocument.name.get_attr_name(), 'name')
        self.assertIs(TestDocument.name.get_parent(), TestDocument)
        self.assert_expression(TestDocument.name, 'test_name')
        self.assertEqual(list(TestDocument.name.fields), [TestDocument.name.raw])
        self.assertEqual(collect_doc_classes(TestDocument.name), {TestDocument})

        self.assertIsInstance(TestDocument.name.raw, AttributedField)
        self.assertIsInstance(TestDocument.name.raw.get_field().get_type(), String)
        self.assert_expression(TestDocument.name.raw, 'test_name.raw')
        self.assertEqual(TestDocument.name.raw.get_field().get_name(), 'test_name.raw')
        self.assertEqual(TestDocument.name.raw.get_attr_name(), 'raw')
        self.assertIsInstance(TestDocument.name.raw.get_parent(), AttributedField)
        self.assertEqual(collect_doc_classes(TestDocument.name.raw), {TestDocument})

        self.assertIsInstance(TestDocument.status, AttributedField)
        self.assertIsInstance(TestDocument.status.get_field().get_type(), Integer)

        self.assertIsInstance(TestDocument.price, AttributedField)
        self.assertIsInstance(TestDocument.price.get_field().get_type(), Float)

        self.assertIsInstance(TestDocument.group, AttributedField)
        self.assertIsInstance(TestDocument.group.get_field().get_type(), Object)
        self.assertEqual(list(TestDocument.group.fields), [TestDocument.group.id, TestDocument.group.name])

        self.assertIsInstance(TestDocument.group.name, AttributedField)
        self.assertEqual(list(TestDocument.group.name.fields), [TestDocument.group.name.raw])
        self.assertEqual(TestDocument.group.name.get_field().get_name(), 'group.test_name')
        self.assertIsInstance(TestDocument.group.name.get_field().get_type(), String)
        self.assertIs(TestDocument.group.name.get_parent(), TestDocument)
        self.assertEqual(collect_doc_classes(TestDocument.group.name), {TestDocument})

        self.assertEqual(TestDocument.group.name.raw.get_attr_name(), 'raw')
        self.assertEqual(TestDocument.group.name.raw.get_field().get_name(), 'group.test_name.raw')
        self.assertIsInstance(TestDocument.group.name.raw.get_field().get_type(), String)
        self.assertIsInstance(TestDocument.group.name.raw.get_parent(), AttributedField)
        self.assertEqual(collect_doc_classes(TestDocument.group.name.raw), {TestDocument})

        self.assertIsInstance(TestDocument.tags, AttributedField)
        self.assertIsInstance(TestDocument.tags.get_field().get_type(), List)
        self.assertEqual(list(TestDocument.tags.fields), [TestDocument.tags.id, TestDocument.tags.name, TestDocument.tags.group])
        self.assertEqual(TestDocument.tags.get_field().get_name(), 'tags')
        self.assert_expression(TestDocument.tags, 'tags')

        self.assertIsInstance(TestDocument.tags.group, AttributedField)
        self.assertIsInstance(TestDocument.tags.group.get_field().get_type(), Object)
        self.assertEqual(list(TestDocument.tags.group.fields), [TestDocument.tags.group.id, TestDocument.tags.group.name])
        self.assertEqual(TestDocument.tags.group.get_field().get_name(), 'tags.group')
        self.assert_expression(TestDocument.tags.group, 'tags.group')

        self.assertIsInstance(TestDocument.tags.group.name, AttributedField)
        self.assertIsInstance(TestDocument.tags.group.name.get_field().get_type(), String)
        self.assertEqual(list(TestDocument.tags.group.name.fields), [TestDocument.tags.group.name.raw])
        self.assertEqual(TestDocument.tags.group.name.get_field().get_name(), 'tags.group.test_name')
        self.assert_expression(TestDocument.tags.group.name, 'tags.group.test_name')

        self.assertIsInstance(TestDocument.tags.group.name.raw, AttributedField)
        self.assertIsInstance(TestDocument.tags.group.name.raw.get_field().get_type(), String)
        self.assertEqual(list(TestDocument.tags.group.name.raw.fields), [])
        self.assertEqual(TestDocument.tags.group.name.raw.get_field().get_name(), 'tags.group.test_name.raw')
        self.assert_expression(TestDocument.tags.group.name.raw, 'tags.group.test_name.raw')

        self.assertIsInstance(TestDocument.tags.group.group_id_1, AttributedField)
        self.assertIsInstance(TestDocument.tags.group.group_id_1.get_field().get_type(), Integer)
        self.assertEqual(list(TestDocument.tags.group.group_id_1.fields), [])
        self.assertEqual(TestDocument.tags.group.group_id_1.get_field().get_name(), 'tags.group.group_id_1')
        self.assert_expression(TestDocument.tags.group.group_id_1, 'tags.group.group_id_1')

        self.assertRaises(AttributeError, lambda: TestDocument.group._id)
        self.assertRaises(KeyError, lambda: TestDocument.group.fields['_id'])

        self.assertRaises(AttributeError, lambda: TestDocument.group.missing_field)
        self.assertRaises(KeyError, lambda: TestDocument.group.fields['missing_field'])

        self.assertIsInstance(TestDocument.i_attr_2, AttributedField)
        self.assertIsInstance(TestDocument.i_attr_2.get_field().get_type(), Integer)
        self.assertEqual(collect_doc_classes(TestDocument.i_attr_2), {TestDocument})
        self.assertEqual(TestDocument.i_attr_2.get_field().get_name(), 'i_attr_2')
        self.assert_expression(TestDocument.i_attr_2, 'i_attr_2')

        self.assertIsInstance(TestDocument.b_attr_1, AttributedField)
        self.assertIsInstance(TestDocument.b_attr_1.get_field().get_type(), Boolean)
        self.assertEqual(collect_doc_classes(TestDocument.b_attr_1), {TestDocument})
        self.assertEqual(TestDocument.b_attr_1.get_field().get_name(), 'b_attr_1')
        self.assert_expression(TestDocument.b_attr_1, 'b_attr_1')

        self.assertRaises(AttributeError, lambda: TestDocument.fake_attr_1)

        self.assertIsInstance(TestDocument.wildcard('date_*'), AttributedField)
        self.assertIsInstance(TestDocument.wildcard('date_*').get_field().get_type(), Type)
        self.assert_expression(TestDocument.wildcard('date_*'), 'date_*')
        self.assertEqual(TestDocument.wildcard('date_*').get_field().get_name(), 'date_*')
        self.assertEqual(collect_doc_classes(TestDocument.wildcard('date_*')), {TestDocument})

        self.assertIsInstance(TestDocument.group.wildcard('date_*'), AttributedField)
        self.assertIsInstance(TestDocument.group.wildcard('date_*').get_field().get_type(), Type)
        self.assert_expression(TestDocument.group.wildcard('date_*'), 'group.date_*')
        self.assertEqual(TestDocument.group.wildcard('date_*').get_field().get_name(), 'group.date_*')
        self.assertEqual(collect_doc_classes(TestDocument.group.wildcard('date_*')), {TestDocument})

        self.assertIsInstance(TestDocument.wildcard('group_*').id, AttributedField)
        self.assertIsInstance(TestDocument.wildcard('group_*').id.get_field().get_type(), Type)
        self.assert_expression(TestDocument.wildcard('group_*').id, 'group_*.id')
        self.assertEqual(TestDocument.wildcard('group_*').id.get_field().get_name(), 'group_*.id')
        self.assertEqual(collect_doc_classes(TestDocument.wildcard('group_*').id), {TestDocument})

        self.assertIsInstance(TestDocument.tags.group.dynamic_fields['group_id_*'], AttributedField)
        self.assertIsInstance(TestDocument.tags.group.dynamic_fields['group_id_*'].get_type(), Integer)
        self.assert_expression(TestDocument.tags.group.dynamic_fields['group_id_*'], 'tags.group.group_id_*')
        self.assertEqual(TestDocument.tags.group.dynamic_fields['group_id_*'].get_field().get_name(), 'tags.group.group_id_*')
        self.assertEqual(collect_doc_classes(TestDocument.tags.group.dynamic_fields['group_id_*']), {TestDocument})

        self.assertRaises(KeyError, lambda: TestDocument.tags.group.dynamic_fields['*'])

        self.assertIs(TestDocument._id, TestDocument._id)
        self.assertIs(TestDocument.name, TestDocument.name)
        self.assertIs(TestDocument.group.name, TestDocument.group.name)
        self.assertIs(TestDocument.tags.group.name, TestDocument.tags.group.name)
        self.assertIs(TestDocument.tags.group.name.raw, TestDocument.tags.group.name.raw)
        # TODO: May be we should cache dynamic fields?
        self.assertIsNot(TestDocument.i_attr_2, TestDocument.i_attr_2)
        self.assertIsNot(TestDocument._id, GroupDocument._id)
        self.assertIsNot(GroupDocument.name, TestDocument.group.name)
        self.assertIsNot(GroupDocument.name, TestDocument.tags.group.name)
        self.assertIsNot(TestDocument.group.name, TestDocument.tags.group.name)
        self.assertIsNot(TagDocument.name, TestDocument.tags.name)

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
                    'group': {'test_name': 'Test group'},
                    'price': 101.5,
                    'tags': [{'id': 1, 'name': 'Test tag'},
                             {'id': 2, 'name': 'Just tag'}],
                    'date_created': '2014-08-14T14:05:28.789Z',
                },
                'highlight': {
                    'test_name': '<em>Test</em> name'
                },
                'matched_queries': ['field_1', 'field_2']
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
        self.assertEqual(hit_doc.get_highlight(), {'test_name': '<em>Test</em> name'})
        self.assertIn('field_1', hit_doc.get_matched_queries())
        self.assertIn('field_2', hit_doc.get_matched_queries())

        hit_doc = TestDocument(
            _hit={
                '_id':'123',
                '_score': 1.23,
                'fields': {
                    'test_name': ['Test name'],
                    'status': [0],
                    'group.test_name': ['Test group'],
                    'price': [101.5],
                    'tags.id': [1, 2],
                    'tags.name': ['Test tag', 'Just tag'],
                    'date_created': ['2014-08-14T14:05:28.789Z'],
                    'not_mapped': ['Test'],
                }
            }
        )
        self.assertEqual(hit_doc._id, '123')
        self.assertAlmostEqual(hit_doc._score, 1.23)
        self.assertEqual(
            hit_doc.get_hit_fields(),
            {
                'test_name': ['Test name'],
                'status': [0],
                'group.test_name': ['Test group'],
                'price': [101.5],
                'tags.id': [1, 2],
                'tags.name': ['Test tag', 'Just tag'],
                'date_created': [datetime.datetime(2014, 8, 14, 14, 5, 28, 789000, dateutil.tz.tzutc())],
                'not_mapped': ['Test'],
            }
        )

        hit_doc = TestDocument(
            _hit={
                '_id':'123'
            }
        )
        self.assertEqual(hit_doc._id, '123')
        self.assertIs(hit_doc.name, None)
        self.assertEqual(hit_doc.get_highlight(), {})

        doc = TestDocument(_id=123, name='Test name', status=0,
                           group=GroupDocument(name='Test group'),
                           price=101.5,
                           tags=[TagDocument(id=1, name='Test tag'),
                                 TagDocument(id=2, name='Just tag')],
                           i_attr_1=None,
                           i_attr_2='',
                           i_attr_3=[],
                           i_attr_4=45)
        self.assertEqual(
            doc.to_source(),
            {
                'test_name': 'Test name',
                'status': 0,
                'group': {
                    'test_name': 'Test group'
                },
                'price': 101.5,
                'tags': [
                    {'id': 1, 'name': 'Test tag'},
                    {'id': 2, 'name': 'Just tag'},
                ],
                'i_attr_4': 45
            }
        )

    def test_inheritance(self):
        class InheritedDocument(TestDocument):
            description = Field(String)

        self.assertSameElements(
            list(InheritedDocument.fields),
            [
                InheritedDocument._uid,
                InheritedDocument._id,
                InheritedDocument._type,
                InheritedDocument._source,
                InheritedDocument._analyzer,
                InheritedDocument._boost,
                InheritedDocument._parent,
                InheritedDocument._field_names,
                InheritedDocument._routing,
                InheritedDocument._index,
                InheritedDocument._size,
                InheritedDocument._timestamp,
                InheritedDocument._ttl,
                InheritedDocument._version,
                InheritedDocument._score,
                InheritedDocument._all,
                InheritedDocument.name,
                InheritedDocument.status,
                InheritedDocument.group,
                InheritedDocument.price,
                InheritedDocument.tags,
                InheritedDocument.date_created,
                InheritedDocument.unused,
                InheritedDocument.description,
            ]
        )
        self.assertIsInstance(InheritedDocument.name, AttributedField)
        self.assertIsInstance(InheritedDocument.name.get_field().get_type(), String)
        self.assertEqual(collect_doc_classes(InheritedDocument.name), {InheritedDocument})
        self.assertIsInstance(InheritedDocument.name.raw, AttributedField)
        self.assertIsInstance(InheritedDocument.name.raw.get_field().get_type(), String)
        self.assertEqual(collect_doc_classes(InheritedDocument.name.raw), {InheritedDocument})
        self.assertIsInstance(InheritedDocument.description, AttributedField)
        self.assertEqual(collect_doc_classes(InheritedDocument.description), {InheritedDocument})

        doc = InheritedDocument(_id=123)
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertIs(doc.name, None)
        self.assertIs(doc.status, None)
        self.assertIs(doc.description, None)
        self.assertEqual(
            doc.to_source(),
            {}
        )

        doc = InheritedDocument(_id=123, status=0, name='Test', i_attr_1=1, i_attr_2=2, face_attr_3=3)
        self.assertEqual(
            doc.to_source(),
            {
                'status': 0,
                'test_name': 'Test',
                'i_attr_1': 1,
                'i_attr_2': 2,
            }
        )

    def test_dynamic_document(self):
        self.assertIsInstance(DynamicDocument._id, AttributedField)
        self.assertNotIsInstance(DynamicDocument._id, DynamicAttributedField)
        self.assertIsInstance(DynamicDocument._id.get_field().get_type(), String)
        self.assertEqual(collect_doc_classes(DynamicDocument._id), {DynamicDocument})
        self.assertIsInstance(DynamicDocument.name, DynamicAttributedField)
        self.assertIsInstance(DynamicDocument.name.get_field().get_type(), Type)
        self.assert_expression(DynamicDocument.name, 'name')
        self.assertEqual(collect_doc_classes(DynamicDocument.name), {DynamicDocument})
        self.assertIsInstance(DynamicDocument.status, DynamicAttributedField)
        self.assertIsInstance(DynamicDocument.status.get_field().get_type(), Type)
        self.assert_expression(DynamicDocument.status, 'status')
        self.assertEqual(collect_doc_classes(DynamicDocument.status), {DynamicDocument})
        self.assertIsInstance(DynamicDocument.group, DynamicAttributedField)
        self.assertIsInstance(DynamicDocument.group.get_field().get_type(), Type)
        self.assert_expression(DynamicDocument.group, 'group')
        self.assertEqual(collect_doc_classes(DynamicDocument.group), {DynamicDocument})
        self.assertIsInstance(DynamicDocument.group.name, DynamicAttributedField)
        self.assertIsInstance(DynamicDocument.group.name.get_field().get_type(), Type)
        self.assertEqual(DynamicDocument.group.name.get_field().get_name(), 'group.name')
        self.assert_expression(DynamicDocument.group.name, 'group.name')
        self.assertEqual(collect_doc_classes(DynamicDocument.group.name), {DynamicDocument})
        self.assertIsInstance(DynamicDocument.group.name.raw, DynamicAttributedField)
        self.assertIsInstance(DynamicDocument.group.name.raw.get_field().get_type(), Type)
        self.assertEqual(DynamicDocument.group.name.raw.get_field().get_name(), 'group.name.raw')
        self.assert_expression(DynamicDocument.group.name.raw, 'group.name.raw')
        self.assertEqual(collect_doc_classes(DynamicDocument.group.name.raw), {DynamicDocument})

    def test_to_mapping(self):
        class ProductGroupDocument(Document):
            __doc_type__ = 'product_group'

            id = Field(Integer)
            name = Field(String, norms={'enabled': False})

        self.assertEqual(
            ProductGroupDocument.to_mapping(),
            {
                "product_group": {
                    "properties": {
                        "id": {
                            "type": "integer"
                        },
                        "name": {
                            "type": "string",
                            "norms": {
                                "enabled": False
                            }
                        }
                    }
                }
            }
        )

        class ProductGroupSubDocument(Document):
            id = Field(Integer)
            name = Field(String, norms={'enabled': False})

            __dynamic_fields__ = [
                Field('group_id_level_*', Integer)
            ]

            __mapping_options__ = {
                'dynamic': True,
            }

        class ProductDocument(Document):
            __doc_type__ = 'product'

            __mapping_options__ = {
                'dynamic': False,
                'date_detection': False,
            }

            _routing = Field(required=True, path='company_id')
            _all = Field(enabled=False)

            name = Field(
                String,
                fields={
                    'autocomplete': Field(
                        String,
                        analyzer='ngrams',
                        search_analyzer='text_delimit',
                        norms={'enabled': False}
                    )
                }
            )
            company_id = Field(Integer)
            group = Field(Object(ProductGroupSubDocument))
            popularity = Field(Float, doc_values=True)

            __dynamic_fields__ = [
                Field('attr_*', Integer)
            ]

        ProductDocument.tags = Field(List(String))

        self.assertEqual(
            ProductDocument.to_mapping(),
            {
                "product": {
                    "dynamic": False,
                    "date_detection": False,
                    "dynamic_templates": [
                        {
                            "group.group_id_level_*": {
                                "path_match": "group.group_id_level_*",
                                "mapping": {
                                    "type": "integer"
                                }
                            }
                        },
                        {
                            "attr_*": {
                                "path_match": "attr_*",
                                "mapping": {
                                    "type": "integer"
                                }
                            }
                        }
                    ],
                    "_routing": {
                        "required": True,
                        "path": "company_id"
                    },
                    "_all": {
                        "enabled": False
                    },
                    "properties": {
                        "name": {
                            "type": "string",
                            "fields": {
                                "autocomplete": {
                                    "type": "string",
                                    "analyzer": "ngrams",
                                    "search_analyzer": "text_delimit",
                                    "norms": {"enabled": False}
                                }
                            }
                        },
                        "company_id": {
                            "type": "integer"
                        },
                        "group": {
                            "dynamic": True,
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "integer"
                                },
                                "name": {
                                    "type": "string",
                                    "norms": {
                                        "enabled": False
                                    }
                                }
                            }
                        },
                        "popularity": {
                            "type": "float",
                            "doc_values": True
                        },
                        "tags": {
                            "type": "string"
                        }
                    }
                }
            }
        )

    def test_geo_point_document(self):

        class GeoPointDoc(Document):
            __doc_type__ = 'geo_data'

            pin = Field(GeoPoint)

        self.assertEqual(
            GeoPointDoc.to_mapping(),
            {
                "geo_data": {
                    "properties": {
                        "pin": {
                            "type": "geo_point",
                        },
                    },
                },
            }
        )

    def test_completion_document(self):

        class CompletionDoc(Document):
            __doc_type__ = 'suggest'

            suggest = Field(Completion, payloads=True)

        self.assertEqual(
            CompletionDoc.to_mapping(),
            {
                'suggest': {
                    'properties': {
                        'suggest': {
                            'type': 'completion',
                            'payloads': True,
                        }
                    }
                }
            }
        )

        doc = CompletionDoc()
        self.assertEqual(doc.to_source(validate=True), {})

        doc = CompletionDoc(suggest='complete this')
        self.assertEqual(doc.to_source(validate=True),
                         {'suggest': 'complete this'})
        doc = CompletionDoc(suggest={'input': ['complete', 'complete this'],
                                     'output': 'complete'})
        self.assertEqual(
            doc.to_source(validate=True),
            {
                'suggest': {
                    'input': [
                        'complete',
                        'complete this',
                    ],
                    'output': 'complete',
                }
            }
        )

        doc = CompletionDoc(suggest=['complete', 'this'])
        self.assertRaises(
            ValidationError, lambda: doc.to_source(validate=True))

    def test_to_source_with_validation(self):
        class TestDocument(Document):
            name = Field(String, required=True)
            status = Field(Integer)

        doc = TestDocument(status=1)
        self.assertRaises(ValidationError, lambda: doc.to_source(validate=True))

        doc = TestDocument(name=None, status=1)
        self.assertRaises(ValidationError, lambda: doc.to_source(validate=True))

        doc = TestDocument(name=123, status='4')
        self.assertEqual(
            doc.to_source(validate=True),
            {
                'name': '123',
                'status': 4,
            }
        )

        doc = TestDocument(name=123, status='4 test')
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

        doc = TestDocument(name=123, status=[1, 2])
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

        doc = TestDocument(name=123, status=datetime.datetime.now())
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

        doc = TestDocument(name=123, status=1 << 31)
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)
