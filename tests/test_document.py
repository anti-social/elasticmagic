import datetime

import dateutil

from elasticmagic.util import collect_doc_classes
from elasticmagic.types import (
    Type, String, Integer, Float, Boolean,
    Date, Object, List, GeoPoint, Completion, Percolator,
)
from elasticmagic.types import ValidationError
from elasticmagic.compat import string_types
from elasticmagic.document import Document, DynamicDocument
from elasticmagic.attribute import AttributedField, DynamicAttributedField
from elasticmagic.expression import Field, MultiMatch

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


class ProductDocument(Document):
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
    #     class ProductDocument(Document):
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
            list(ProductDocument.fields),
            [
                ProductDocument._uid,
                ProductDocument._id,
                ProductDocument._type,
                ProductDocument._source,
                ProductDocument._analyzer,
                ProductDocument._boost,
                ProductDocument._parent,
                ProductDocument._field_names,
                ProductDocument._routing,
                ProductDocument._index,
                ProductDocument._size,
                ProductDocument._timestamp,
                ProductDocument._ttl,
                ProductDocument._version,
                ProductDocument._score,
                ProductDocument._all,
                ProductDocument.name,
                ProductDocument.status,
                ProductDocument.group,
                ProductDocument.price,
                ProductDocument.tags,
                ProductDocument.date_created,
                ProductDocument.unused,
            ]
        )
        self.assertSameElements(
            list(ProductDocument.user_fields),
            [
                ProductDocument.name,
                ProductDocument.status,
                ProductDocument.group,
                ProductDocument.price,
                ProductDocument.tags,
                ProductDocument.date_created,
                ProductDocument.unused,
            ]
        )
        self.assertSameElements(
            list(ProductDocument.mapping_fields),
            [
                ProductDocument._uid,
                ProductDocument._id,
                ProductDocument._type,
                ProductDocument._source,
                ProductDocument._analyzer,
                ProductDocument._boost,
                ProductDocument._parent,
                ProductDocument._field_names,
                ProductDocument._routing,
                ProductDocument._index,
                ProductDocument._size,
                ProductDocument._timestamp,
                ProductDocument._ttl,
                ProductDocument._version,
                ProductDocument._score,
                ProductDocument._all,
            ]
        )
        self.assertIsInstance(ProductDocument._id, AttributedField)
        self.assertIsInstance(ProductDocument._id.get_field().get_type(), String)
        self.assertEqual(ProductDocument._id.get_field().get_name(), '_id')
        self.assertEqual(ProductDocument._id.get_attr_name(), '_id')
        self.assertIs(ProductDocument._id.get_parent(), ProductDocument)
        self.assert_expression(ProductDocument._id, '_id')

        self.assertIsInstance(ProductDocument._all, AttributedField)
        self.assertIsInstance(ProductDocument._all.get_field().get_type(), String)
        self.assertEqual(ProductDocument._all.get_field().get_name(), '_all')
        self.assertEqual(ProductDocument._all.get_attr_name(), '_all')
        self.assertIs(ProductDocument._all.get_parent(), ProductDocument)
        self.assert_expression(ProductDocument._all, '_all')

        self.assertIsInstance(ProductDocument._score, AttributedField)
        self.assertIsInstance(ProductDocument._score.get_field().get_type(), Float)
        self.assertEqual(ProductDocument._score.get_field().get_name(), '_score')
        self.assertEqual(ProductDocument._score.get_attr_name(), '_score')
        self.assertIs(ProductDocument._score.get_parent(), ProductDocument)
        self.assert_expression(ProductDocument._score, '_score')

        self.assertIsInstance(ProductDocument.name, AttributedField)
        self.assertIsInstance(ProductDocument.name.get_field().get_type(), String)
        self.assertEqual(ProductDocument.name.get_field().get_name(), 'test_name')
        self.assertEqual(ProductDocument.name.get_attr_name(), 'name')
        self.assertIs(ProductDocument.name.get_parent(), ProductDocument)
        self.assert_expression(ProductDocument.name, 'test_name')
        self.assertEqual(list(ProductDocument.name.fields), [ProductDocument.name.raw])
        self.assertEqual(collect_doc_classes(ProductDocument.name), {ProductDocument})

        self.assertIsInstance(ProductDocument.name.raw, AttributedField)
        self.assertIsInstance(ProductDocument.name.raw.get_field().get_type(), String)
        self.assert_expression(ProductDocument.name.raw, 'test_name.raw')
        self.assertEqual(ProductDocument.name.raw.get_field().get_name(), 'test_name.raw')
        self.assertEqual(ProductDocument.name.raw.get_attr_name(), 'raw')
        self.assertIsInstance(ProductDocument.name.raw.get_parent(), AttributedField)
        self.assertEqual(collect_doc_classes(ProductDocument.name.raw), {ProductDocument})

        self.assertIsInstance(ProductDocument.status, AttributedField)
        self.assertIsInstance(ProductDocument.status.get_field().get_type(), Integer)

        self.assertIsInstance(ProductDocument.price, AttributedField)
        self.assertIsInstance(ProductDocument.price.get_field().get_type(), Float)

        self.assertIsInstance(ProductDocument.group, AttributedField)
        self.assertIsInstance(ProductDocument.group.get_field().get_type(), Object)
        self.assertEqual(list(ProductDocument.group.fields), [ProductDocument.group.id, ProductDocument.group.name])

        self.assertIsInstance(ProductDocument.group.name, AttributedField)
        self.assertEqual(list(ProductDocument.group.name.fields), [ProductDocument.group.name.raw])
        self.assertEqual(ProductDocument.group.name.get_field().get_name(), 'group.test_name')
        self.assertIsInstance(ProductDocument.group.name.get_field().get_type(), String)
        self.assertIs(ProductDocument.group.name.get_parent(), ProductDocument)
        self.assertEqual(collect_doc_classes(ProductDocument.group.name), {ProductDocument})

        self.assertEqual(ProductDocument.group.name.raw.get_attr_name(), 'raw')
        self.assertEqual(ProductDocument.group.name.raw.get_field().get_name(), 'group.test_name.raw')
        self.assertIsInstance(ProductDocument.group.name.raw.get_field().get_type(), String)
        self.assertIsInstance(ProductDocument.group.name.raw.get_parent(), AttributedField)
        self.assertEqual(collect_doc_classes(ProductDocument.group.name.raw), {ProductDocument})

        self.assertIsInstance(ProductDocument.tags, AttributedField)
        self.assertIsInstance(ProductDocument.tags.get_field().get_type(), List)
        self.assertEqual(list(ProductDocument.tags.fields), [ProductDocument.tags.id, ProductDocument.tags.name, ProductDocument.tags.group])
        self.assertEqual(ProductDocument.tags.get_field().get_name(), 'tags')
        self.assert_expression(ProductDocument.tags, 'tags')

        self.assertIsInstance(ProductDocument.tags.group, AttributedField)
        self.assertIsInstance(ProductDocument.tags.group.get_field().get_type(), Object)
        self.assertEqual(list(ProductDocument.tags.group.fields), [ProductDocument.tags.group.id, ProductDocument.tags.group.name])
        self.assertEqual(ProductDocument.tags.group.get_field().get_name(), 'tags.group')
        self.assert_expression(ProductDocument.tags.group, 'tags.group')

        self.assertIsInstance(ProductDocument.tags.group.name, AttributedField)
        self.assertIsInstance(ProductDocument.tags.group.name.get_field().get_type(), String)
        self.assertEqual(list(ProductDocument.tags.group.name.fields), [ProductDocument.tags.group.name.raw])
        self.assertEqual(ProductDocument.tags.group.name.get_field().get_name(), 'tags.group.test_name')
        self.assert_expression(ProductDocument.tags.group.name, 'tags.group.test_name')

        self.assertIsInstance(ProductDocument.tags.group.name.raw, AttributedField)
        self.assertIsInstance(ProductDocument.tags.group.name.raw.get_field().get_type(), String)
        self.assertEqual(list(ProductDocument.tags.group.name.raw.fields), [])
        self.assertEqual(ProductDocument.tags.group.name.raw.get_field().get_name(), 'tags.group.test_name.raw')
        self.assert_expression(ProductDocument.tags.group.name.raw, 'tags.group.test_name.raw')

        self.assertIsInstance(ProductDocument.tags.group.group_id_1, AttributedField)
        self.assertIsInstance(ProductDocument.tags.group.group_id_1.get_field().get_type(), Integer)
        self.assertEqual(list(ProductDocument.tags.group.group_id_1.fields), [])
        self.assertEqual(ProductDocument.tags.group.group_id_1.get_field().get_name(), 'tags.group.group_id_1')
        self.assert_expression(ProductDocument.tags.group.group_id_1, 'tags.group.group_id_1')

        self.assertRaises(AttributeError, lambda: ProductDocument.group._id)
        self.assertRaises(KeyError, lambda: ProductDocument.group.fields['_id'])

        self.assertRaises(AttributeError, lambda: ProductDocument.group.missing_field)
        self.assertRaises(KeyError, lambda: ProductDocument.group.fields['missing_field'])

        self.assertIsInstance(ProductDocument.i_attr_2, AttributedField)
        self.assertIsInstance(ProductDocument.i_attr_2.get_field().get_type(), Integer)
        self.assertEqual(collect_doc_classes(ProductDocument.i_attr_2), {ProductDocument})
        self.assertEqual(ProductDocument.i_attr_2.get_field().get_name(), 'i_attr_2')
        self.assert_expression(ProductDocument.i_attr_2, 'i_attr_2')

        self.assertIsInstance(ProductDocument.b_attr_1, AttributedField)
        self.assertIsInstance(ProductDocument.b_attr_1.get_field().get_type(), Boolean)
        self.assertEqual(collect_doc_classes(ProductDocument.b_attr_1), {ProductDocument})
        self.assertEqual(ProductDocument.b_attr_1.get_field().get_name(), 'b_attr_1')
        self.assert_expression(ProductDocument.b_attr_1, 'b_attr_1')

        self.assertRaises(AttributeError, lambda: ProductDocument.fake_attr_1)

        self.assertIsInstance(ProductDocument.wildcard('date_*'), AttributedField)
        self.assertIsInstance(ProductDocument.wildcard('date_*').get_field().get_type(), Type)
        self.assert_expression(ProductDocument.wildcard('date_*'), 'date_*')
        self.assertEqual(ProductDocument.wildcard('date_*').get_field().get_name(), 'date_*')
        self.assertEqual(collect_doc_classes(ProductDocument.wildcard('date_*')), {ProductDocument})

        self.assertIsInstance(ProductDocument.group.wildcard('date_*'), AttributedField)
        self.assertIsInstance(ProductDocument.group.wildcard('date_*').get_field().get_type(), Type)
        self.assert_expression(ProductDocument.group.wildcard('date_*'), 'group.date_*')
        self.assertEqual(ProductDocument.group.wildcard('date_*').get_field().get_name(), 'group.date_*')
        self.assertEqual(collect_doc_classes(ProductDocument.group.wildcard('date_*')), {ProductDocument})

        self.assertIsInstance(ProductDocument.wildcard('group_*').id, AttributedField)
        self.assertIsInstance(ProductDocument.wildcard('group_*').id.get_field().get_type(), Type)
        self.assert_expression(ProductDocument.wildcard('group_*').id, 'group_*.id')
        self.assertEqual(ProductDocument.wildcard('group_*').id.get_field().get_name(), 'group_*.id')
        self.assertEqual(collect_doc_classes(ProductDocument.wildcard('group_*').id), {ProductDocument})

        self.assertIsInstance(ProductDocument.tags.group.dynamic_fields['group_id_*'], AttributedField)
        self.assertIsInstance(ProductDocument.tags.group.dynamic_fields['group_id_*'].get_type(), Integer)
        self.assert_expression(ProductDocument.tags.group.dynamic_fields['group_id_*'], 'tags.group.group_id_*')
        self.assertEqual(ProductDocument.tags.group.dynamic_fields['group_id_*'].get_field().get_name(), 'tags.group.group_id_*')
        self.assertEqual(collect_doc_classes(ProductDocument.tags.group.dynamic_fields['group_id_*']), {ProductDocument})

        self.assertRaises(KeyError, lambda: ProductDocument.tags.group.dynamic_fields['*'])

        self.assertIs(ProductDocument._id, ProductDocument._id)
        self.assertIs(ProductDocument.name, ProductDocument.name)
        self.assertIs(ProductDocument.group.name, ProductDocument.group.name)
        self.assertIs(ProductDocument.tags.group.name, ProductDocument.tags.group.name)
        self.assertIs(ProductDocument.tags.group.name.raw, ProductDocument.tags.group.name.raw)
        # TODO: May be we should cache dynamic fields?
        self.assertIsNot(ProductDocument.i_attr_2, ProductDocument.i_attr_2)
        self.assertIsNot(ProductDocument._id, GroupDocument._id)
        self.assertIsNot(GroupDocument.name, ProductDocument.group.name)
        self.assertIsNot(GroupDocument.name, ProductDocument.tags.group.name)
        self.assertIsNot(ProductDocument.group.name, ProductDocument.tags.group.name)
        self.assertIsNot(TagDocument.name, ProductDocument.tags.name)

        doc = ProductDocument()
        self.assertIs(doc._id, None)
        self.assertIs(doc.name, None)
        self.assertIs(doc.status, None)
        doc._id = 123
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)

        doc = ProductDocument(_id=123)
        self.assertIsInstance(doc._id, int)
        self.assertEqual(doc._id, 123)
        self.assertIs(doc.name, None)
        self.assertIs(doc.status, None)

        doc = ProductDocument(_id=123, name='Test name', status=0,
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

        hit_doc = ProductDocument(
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

        hit_doc = ProductDocument(
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

        hit_doc = ProductDocument(
            _hit={
                '_id':'123'
            }
        )
        self.assertEqual(hit_doc._id, '123')
        self.assertIs(hit_doc.name, None)
        self.assertEqual(hit_doc.get_highlight(), {})

        doc = ProductDocument(_id=123, name='Test name', status=0,
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
        class InheritedDocument(ProductDocument):
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
        class ProductDocument(Document):
            name = Field(String, required=True)
            status = Field(Integer)

        doc = ProductDocument(status=1)
        self.assertRaises(ValidationError, lambda: doc.to_source(validate=True))

        doc = ProductDocument(name=None, status=1)
        self.assertRaises(ValidationError, lambda: doc.to_source(validate=True))

        doc = ProductDocument(name=123, status='4')
        self.assertEqual(
            doc.to_source(validate=True),
            {
                'name': '123',
                'status': 4,
            }
        )

        doc = ProductDocument(name=123, status='4 test')
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

        doc = ProductDocument(name=123, status=[1, 2])
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

        doc = ProductDocument(name=123, status=datetime.datetime.now())
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

        doc = ProductDocument(name=123, status=1 << 31)
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)

    def test_percolator_field(self):
        class ProductDocument(Document):
            name = Field(String)
            keywords = Field(List(String))

        class QueryDocument(Document):
            __doc_type__ = 'query'

            query = Field(Percolator)

        self.assertEqual(
            QueryDocument.to_mapping(),
            {
                "query": {
                    "properties": {
                        "query": {"type": "percolator"}
                    }
                }
            }
        )

        doc = QueryDocument(
            query=MultiMatch(
                "Super deal",
                [ProductDocument.name.boost(1.5), ProductDocument.keywords],
                type='cross_fields'))
        self.assertEqual(
            doc.to_source(),
            {
                "query": {
                    "multi_match": {
                        "type": "cross_fields",
                        "query": "Super deal",
                        "fields": ["name^1.5", "keywords"],
                    }
                }
            }
        )

        doc = QueryDocument(query='test')
        with self.assertRaises(ValidationError):
            doc.to_source(validate=True)
