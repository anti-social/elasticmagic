import datetime

import dateutil

from elasticmagic.attribute import AttributedField, DynamicAttributedField
from elasticmagic.compiler import all_compilers
from elasticmagic.document import Document, DynamicDocument
from elasticmagic.expression import Field, MultiMatch
from elasticmagic.util import collect_doc_classes
from elasticmagic.types import (
    Type, String, Integer, Float, Boolean,
    Date, Object, List, GeoPoint, Completion, Percolator,
)
from elasticmagic.types import ValidationError

import pytest

from .conftest import assert_expression
from .conftest import assert_same_elements


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


class InheritedDocument(ProductDocument):
    description = Field(String)


class CompletionDoc(Document):
    __doc_type__ = 'suggest'

    suggest = Field(Completion, payloads=True)


class QueryDocument(Document):
    __doc_type__ = 'query'

    query = Field(Percolator)


def test_base_document():
    assert_same_elements(Document.fields, Document.mapping_fields)
    assert_same_elements(
        Document.fields,
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


def test_document_class_fields_are_the_same():
    assert_same_elements(
        ProductDocument.fields,
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
    assert_same_elements(
        ProductDocument.user_fields,
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
    assert_same_elements(
        ProductDocument.mapping_fields,
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


@pytest.mark.parametrize('field,field_type,field_name', [
    (ProductDocument._id, String, '_id'),
    (ProductDocument._uid, String, '_uid'),
    (ProductDocument._type, String, '_type'),
    (ProductDocument._index, String, '_index'),
    (ProductDocument._source, String, '_source'),
    (ProductDocument._version, Integer, '_version'),
    (ProductDocument._all, String, '_all'),
    (ProductDocument._score, Float, '_score'),
])
def test_document_class_meta_fields(field, field_type, field_name):
    assert isinstance(field, AttributedField)
    assert isinstance(field.get_field().get_type(), field_type)
    assert field.get_field().get_name() == field_name
    assert field.get_attr_name() == field_name
    assert field.get_parent() is ProductDocument
    # assert_expression(field, field_name, compiler)


@pytest.mark.parametrize(
    'field,field_type,field_name,mapping_name,parent,subfields', [
        (
            ProductDocument.name, String, 'name', 'test_name',
            ProductDocument, [ProductDocument.name.raw]
        ),
        (
            ProductDocument.name.raw, String, 'raw', 'test_name.raw',
            ProductDocument.name, []
        ),
        (
            ProductDocument.status, Integer, 'status', 'status',
            ProductDocument, []
        ),
        (
            ProductDocument.price, Float, 'price', 'price',
            ProductDocument, []
        ),
        (
            ProductDocument.group, Object, 'group', 'group',
            ProductDocument,
            [ProductDocument.group.id, ProductDocument.group.name]
        ),
        (
            ProductDocument.group.name, String, 'name', 'group.test_name',
            ProductDocument, [ProductDocument.group.name.raw]
        ),
        (
            ProductDocument.group.name.raw, String,
            'raw', 'group.test_name.raw',
            ProductDocument.group.name, []
        ),
        (
            ProductDocument.tags, List, 'tags', 'tags',
            ProductDocument,
            [
                ProductDocument.tags.id,
                ProductDocument.tags.name,
                ProductDocument.tags.group
            ]
        ),
        (
            ProductDocument.tags.group, Object, 'group', 'tags.group',
            ProductDocument,
            [ProductDocument.tags.group.id, ProductDocument.tags.group.name]
        ),
        (
            ProductDocument.tags.group.name, String,
            'name', 'tags.group.test_name',
            ProductDocument, [ProductDocument.tags.group.name.raw]
        ),
        (
            ProductDocument.tags.group.name.raw, String,
            'raw', 'tags.group.test_name.raw',
            ProductDocument.tags.group.name, []
        ),
        (
            ProductDocument.tags.group.group_id_1, Integer,
            'group_id_1', 'tags.group.group_id_1',
            ProductDocument, []
        ),
        (
            ProductDocument.i_attr_2, Integer, 'i_attr_2', 'i_attr_2',
            ProductDocument, []
        ),
        (
            ProductDocument.b_attr_2, Boolean, 'b_attr_2', 'b_attr_2',
            ProductDocument, []
        ),
        (
            ProductDocument.wildcard('date_*'), Type, 'date_*', 'date_*',
            ProductDocument, []
        ),
        (
            ProductDocument.group.wildcard('date_*'), Type,
            'date_*', 'group.date_*',
            ProductDocument, []
        ),
        (
            ProductDocument.wildcard('group_*').id, Type,
            'id', 'group_*.id',
            ProductDocument, []
        ),
        (
            ProductDocument.tags.group.dynamic_fields['group_id_*'], Integer,
            'group_id_*', 'tags.group.group_id_*',
            ProductDocument, []
        ),
    ]
)
def test_document_class_user_fields(
        field, field_type, field_name, mapping_name, parent, subfields,
):
    assert isinstance(field, AttributedField)
    assert isinstance(field.get_field().get_type(), field_type)
    assert field.get_attr_name() == field_name
    assert field.get_field().get_name() == mapping_name
    assert field.get_parent() is parent
    assert_same_elements(field.fields, subfields)
    assert collect_doc_classes(field) == {ProductDocument}
    for compiler in all_compilers:
        assert_expression(field, mapping_name, compiler)


def test_document_class_missing_fields():
    with pytest.raises(AttributeError):
        ProductDocument.missing_field
    with pytest.raises(AttributeError):
        ProductDocument.group._id
    with pytest.raises(KeyError):
        ProductDocument.group.fields['_id']
    with pytest.raises(KeyError):
        ProductDocument.tags.group.dynamic_fields['*']


def test_document_class_user_fields_identity():
    assert ProductDocument._id is ProductDocument._id
    assert ProductDocument.name is ProductDocument.name
    assert ProductDocument.group.name is ProductDocument.group.name
    assert ProductDocument.tags.group.name is ProductDocument.tags.group.name
    assert ProductDocument.tags.group.name.raw is \
        ProductDocument.tags.group.name.raw

    # TODO: May be we should cache dynamic fields?
    assert ProductDocument.i_attr_2 is not ProductDocument.i_attr_2

    assert ProductDocument._id is not GroupDocument._id
    assert GroupDocument.name is not ProductDocument.group.name
    assert GroupDocument.name is not ProductDocument.tags.group.name
    assert ProductDocument.group.name is not ProductDocument.tags.group.name
    assert TagDocument.name is not ProductDocument.tags.name


def test_document_instance__empty():
    doc = ProductDocument()
    for field in ProductDocument.fields:
        assert getattr(doc, field.get_attr_name()) is None

    doc._id = 123
    assert doc._id == 123
    doc.name = 'Test name'
    assert doc.name == 'Test name'


def test_document_instance__only_id():
    doc = ProductDocument(_id=123)
    assert doc._id == 123
    for field in ProductDocument.fields:
        if field is ProductDocument._id:
            continue
        assert getattr(doc, field.get_attr_name()) is None


def test_document_instance__some_user_fields():
    doc = ProductDocument(
        _id=123,
        name='Test name',
        status=0,
        group=GroupDocument(name='Test group'),
        price=99.99,
        tags=[
            TagDocument(id=1, name='Test tag'),
            TagDocument(id=2, name='Just tag'),
        ]
    )
    assert doc._id == 123
    assert doc.name == 'Test name'
    assert doc.status == 0
    assert isinstance(doc.group, GroupDocument)
    assert doc.price == 99.99
    assert doc.group.name == 'Test group'
    assert isinstance(doc.tags, list)
    assert doc.tags[0].id == 1
    assert doc.tags[0].name == 'Test tag'
    assert doc.tags[1].id == 2
    assert doc.tags[1].name == 'Just tag'


def test_document_instance__from_hit():
    hit_doc = ProductDocument(
        _hit={
            '_id': '123',
            '_score': 1.23,
            '_source': {
                'test_name': 'Test name',
                'status': 0,
                'group': {'test_name': 'Test group'},
                'price': 101.5,
                'tags': [
                    {'id': 1, 'name': 'Test tag'},
                    {'id': 2, 'name': 'Just tag'}
                ],
                'date_created': '2014-08-14T14:05:28.789Z',
            },
            'highlight': {
                'test_name': '<em>Test</em> name'
            },
            'matched_queries': ['field_1', 'field_2'],
            '_explanation': {
                'value': 3.14,
                'description': 'pi',
            },
            'sort': [
                1675636742438,
                '1760183056'
            ]
        }
    )
    assert hit_doc._id == '123'
    assert hit_doc._score == 1.23
    assert hit_doc.name == 'Test name'
    assert hit_doc.status == 0
    assert isinstance(hit_doc.group, GroupDocument)
    assert hit_doc.group.name == 'Test group'
    assert hit_doc.price == 101.5
    assert isinstance(hit_doc.tags, list)
    assert isinstance(hit_doc.tags[0], TagDocument)
    assert hit_doc.tags[0].id == 1
    assert hit_doc.tags[0].name == 'Test tag'
    assert hit_doc.tags[1].id == 2
    assert hit_doc.tags[1].name == 'Just tag'
    assert hit_doc.date_created == \
        datetime.datetime(2014, 8, 14, 14, 5, 28, 789000, dateutil.tz.tzutc())
    assert hit_doc.unused is None
    assert hit_doc.get_highlight() == {'test_name': '<em>Test</em> name'}
    assert hit_doc.get_matched_queries() == ['field_1', 'field_2']
    assert hit_doc.get_explanation() == {'value': 3.14, 'description': 'pi'}
    assert hit_doc.get_sort_values() == [1675636742438, '1760183056']


def test_document_instance__from_hit_with_fields():
    hit_doc = ProductDocument(
        _hit={
            '_id': '123',
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
    assert hit_doc._id == '123'
    assert hit_doc._score == 1.23
    assert hit_doc.get_hit_fields() == {
        'test_name': ['Test name'],
        'status': [0],
        'group.test_name': ['Test group'],
        'price': [101.5],
        'tags.id': [1, 2],
        'tags.name': ['Test tag', 'Just tag'],
        'date_created': [
            datetime.datetime(
                2014, 8, 14, 14, 5, 28, 789000, dateutil.tz.tzutc()
            )
        ],
        'not_mapped': ['Test'],
    }
    assert hit_doc.get_sort_values() == []


def test_document_instance__from_hit_without_source():
    hit_doc = ProductDocument(
        _hit={
            '_id': '123'
        }
    )
    assert hit_doc._id == '123'
    for field in ProductDocument.fields:
        if field is ProductDocument._id:
            continue
        assert hit_doc.name is None
    assert hit_doc.get_highlight() == {}
    assert hit_doc.get_hit_fields() == {}


def test_document_instance__to_source(compiler):
    doc = ProductDocument(
        _id=123,
        name='Test name',
        status=0,
        group=None,
        price=101.5,
        tags=[
            TagDocument(id=1, name='Test tag'),
            TagDocument(id=2, name=None)
        ],
        i_attr_1=None,
        i_attr_2='',
        i_attr_3=[],
        i_attr_4=45,
    )
    assert doc.to_source(compiler) == {
        'test_name': 'Test name',
        'status': 0,
        'group': None,
        'price': 101.5,
        'tags': [
            {'id': 1, 'name': 'Test tag'},
            {'id': 2, 'name': None},
        ],
        'i_attr_1': None,
        'i_attr_2': '',
        'i_attr_3': [],
        'i_attr_4': 45
    }


def test_document_class_inheritance():
    assert_same_elements(
        InheritedDocument.fields,
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
    for field in ProductDocument.fields:
        assert getattr(InheritedDocument, field.get_attr_name()) is not field

    assert isinstance(InheritedDocument.name, AttributedField)
    assert isinstance(InheritedDocument.name.get_field().get_type(), String)
    assert collect_doc_classes(InheritedDocument.name) == {InheritedDocument}

    assert isinstance(InheritedDocument.name.raw, AttributedField)
    assert isinstance(
        InheritedDocument.name.raw.get_field().get_type(), String
    )
    assert collect_doc_classes(InheritedDocument.name.raw) == \
        {InheritedDocument}

    assert isinstance(InheritedDocument.description, AttributedField)
    assert isinstance(
        InheritedDocument.description.get_field().get_type(), String
    )
    assert collect_doc_classes(InheritedDocument.description) == \
        {InheritedDocument}

    doc = InheritedDocument(_id=123)
    assert doc._id == 123
    for field in InheritedDocument.fields:
        if field is InheritedDocument._id:
            continue
        assert getattr(doc, field.get_attr_name()) is None


def test_inherited_document_instance__to_source(compiler):
    doc = InheritedDocument(_id=123)
    assert doc.to_source(compiler) == {}

    doc = InheritedDocument(
        _id=123, status=0, name='Test', i_attr_1=1, i_attr_2=2, face_attr_3=3
    )
    assert doc.to_source(compiler) == {
        'status': 0,
        'test_name': 'Test',
        'i_attr_1': 1,
        'i_attr_2': 2,
    }


@pytest.mark.parametrize('field,field_type,field_name', [
    (DynamicDocument._id, String, '_id'),
])
def test_dynamic_document_class__mapping_fields(field, field_type, field_name):
    assert isinstance(field, AttributedField)
    assert not isinstance(field, DynamicAttributedField)
    assert isinstance(field.get_field().get_type(), String)
    assert collect_doc_classes(field) == {DynamicDocument}


@pytest.mark.parametrize('field,field_name', [
    (DynamicDocument.name, 'name'),
    (DynamicDocument.status, 'status'),
    (DynamicDocument.group, 'group'),
    (DynamicDocument.group.name, 'group.name'),
    (DynamicDocument.group.name.raw, 'group.name.raw'),
    (DynamicDocument.wildcard('name.*'), 'name.*'),
    (DynamicDocument.name.wildcard('*'), 'name.*'),
    (DynamicDocument.group.name.wildcard('*'), 'group.name.*'),
])
def test_dynamic_document_class__user_fields(field, field_name):
    assert isinstance(field, DynamicAttributedField)
    assert isinstance(field.get_field().get_type(), Type)
    assert field.get_field().get_name() == field_name
    assert collect_doc_classes(field) == {DynamicDocument}
    for compiler in all_compilers:
        assert_expression(field, field_name, compiler)


def test_dynamic_document_instance__to_source(compiler):
    doc = DynamicDocument()
    assert doc.to_source(compiler) == {}

    doc = DynamicDocument()
    doc.name = 'Doc with unknown status'
    doc.status = None
    doc.status_name = ''
    doc.tags = []
    assert doc.to_source(compiler) == {
        'name': 'Doc with unknown status',
        'status': None,
        'status_name': '',
        'tags': [],
    }

    doc = DynamicDocument(name='Test', status=1, _internal_status=2)
    assert doc.to_source(compiler) == {
        'name': 'Test',
        'status': 1,
        '_internal_status': 2,
    }

    doc = DynamicDocument(
        _hit={'_source': {'name': 'Doc from source', 'status': None}}
    )
    assert doc.to_source(compiler) == {
        'name': 'Doc from source',
        'status': None,
    }


def test_document_class__to_mapping(compiler):
    class ProductGroupDocument(Document):
        __doc_type__ = 'product_group'

        id = Field(Integer)
        name = Field(String, norms={'enabled': False})

    expeted_mapping = {
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
    assert ProductGroupDocument.to_mapping(compiler) == expeted_mapping


def test_document_class_with_subdocument__to_mapping(compiler):
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

    # add field on the fly
    ProductDocument.tags = Field(List(String))

    expected_mapping = {
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
    assert ProductDocument.to_mapping(compiler) == expected_mapping


def test_document_class_with_geopoint_field__to_mapping(compiler):
    class GeoPointDoc(Document):
        __doc_type__ = 'geo_data'

        pin = Field(GeoPoint)

    expected_mapping = {
        "properties": {
            "pin": {
                "type": "geo_point",
            },
        }
    }
    assert GeoPointDoc.to_mapping(compiler) == expected_mapping


def test_document_class_with_completion_field__to_mapping(compiler):
    assert CompletionDoc.to_mapping(compiler) == {
        'properties': {
            'suggest': {
                'type': 'completion',
                'payloads': True,
            }
        }
    }


def test_document_instance_with_completion_field__to_source(compiler):
    doc = CompletionDoc()
    assert doc.to_source(compiler, validate=True) == {}

    doc = CompletionDoc(suggest='complete this')
    assert doc.to_source(compiler, validate=True) == {
        'suggest': 'complete this'
    }

    doc = CompletionDoc(
        suggest={
            'input': ['complete', 'complete this'],
            'output': 'complete'
        }
    )
    assert doc.to_source(compiler, validate=True) == {
        'suggest': {
            'input': [
                'complete',
                'complete this',
            ],
            'output': 'complete',
        }
    }

    doc = CompletionDoc(suggest=['complete', 'this'])
    with pytest.raises(ValidationError):
        doc.to_source(compiler, validate=True)


def test_document_class_with_percolator_field__to_mapping(compiler):
    assert QueryDocument.to_mapping(compiler) == {
        "properties": {
            "query": {"type": "percolator"}
        }
    }


def test_document_instance_with_percolator_field__to_source(compiler):
    class ProductDocument(Document):
        name = Field(String)
        keywords = Field(List(String))

    doc = QueryDocument(
        query=MultiMatch(
            "Super deal",
            [ProductDocument.name.boost(1.5), ProductDocument.keywords],
            type='cross_fields')
    )
    assert doc.to_source(compiler) == {
        "query": {
            "multi_match": {
                "type": "cross_fields",
                "query": "Super deal",
                "fields": ["name^1.5", "keywords"],
            }
        }
    }

    doc = QueryDocument(query='test')
    with pytest.raises(ValidationError):
        doc.to_source(compiler, validate=True)


def test_document_instance__to_source_with_validation(compiler):
    class ProductDocument(Document):
        name = Field(String, required=True)
        status = Field(Integer)

    with pytest.raises(ValidationError):
        doc = ProductDocument(status=1)
        doc.to_source(compiler, validate=True)

    with pytest.raises(ValidationError):
        doc = ProductDocument(name=None, status=1)
        doc.to_source(compiler, validate=True)

    doc = ProductDocument(name=123, status='4')
    assert doc.to_source(compiler, validate=True) == {
        'name': '123',
        'status': 4,
    }

    with pytest.raises(ValidationError):
        doc = ProductDocument(name=123, status='4 test')
        doc.to_source(compiler, validate=True)

    with pytest.raises(ValidationError):
        doc = ProductDocument(name=123, status=[1, 2])
        doc.to_source(compiler, validate=True)

    with pytest.raises(ValidationError):
        doc = ProductDocument(name=123, status=datetime.datetime.now())
        doc.to_source(compiler, validate=True)

    with pytest.raises(ValidationError):
        doc = ProductDocument(name=123, status=1 << 31)
        doc.to_source(compiler, validate=True)
