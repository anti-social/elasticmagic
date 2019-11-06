from .types import Type, String, Integer, Float, Date
from .attribute import AttributedField, DynamicAttributedField
from .attribute import _attributed_field_factory
from .expression import Field, MappingField
from .datastructures import OrderedAttributes
from .util import cached_property
from .compat import with_metaclass


DOC_TYPE_FIELD = '_doc_type'
DOC_TYPE_NAME_FIELD = '{}.name'.format(DOC_TYPE_FIELD)
DOC_TYPE_PARENT_FIELD = '{}.parent'.format(DOC_TYPE_FIELD)
DOC_TYPE_JOIN_FIELD = '_doc_type_join'
DOC_TYPE_ID_DELIMITER = '~'
DOC_TYPE_PARENT_DELIMITER = '#'


def mk_uid(doc_type, doc_id):
    return '{}{}{}'.format(doc_type, DOC_TYPE_ID_DELIMITER, doc_id)


def get_doc_type_for_hit(hit):
    fields = hit.get('fields', {})
    custom_doc_type = fields.get(DOC_TYPE_NAME_FIELD)
    if custom_doc_type:
        return custom_doc_type[0]
    return hit['_type']


class DocumentMeta(type):
    def __new__(meta, name, bases, dct):
        cls = type.__new__(meta, name, bases, dct)

        cls._dynamic_defaults = cls._get_dynamic_defaults()

        cls._fields = OrderedAttributes(defaults=cls._dynamic_defaults)
        cls._user_fields = OrderedAttributes(defaults=cls._dynamic_defaults)
        cls._mapping_fields = OrderedAttributes()
        cls._dynamic_fields = OrderedAttributes()
        cls._field_name_map = {}

        process_fields = []

        for attr_name in dir(cls):
            field = getattr(cls, attr_name)
            if isinstance(field, AttributedField):
                if field._attr_name not in cls.__dict__:
                    # inherited from base document class
                    process_fields.append((attr_name, field._field))
            elif isinstance(field, Field):
                process_fields.append((attr_name, field))
        process_fields = sorted(process_fields, key=lambda v: v[1]._count)

        for attr_name, field in process_fields:
            if attr_name in cls.__dict__:
                delattr(cls, attr_name)
            setattr(cls, attr_name, field)

        for dyn_field in cls.__dynamic_fields__:
            cls._dynamic_fields[dyn_field.get_name()] = AttributedField(
                cls, dyn_field.get_name(), dyn_field
            )

        return cls

    def _get_dynamic_defaults(cls):
        dynamic_defaults = {}
        for dyn_field in cls.__dynamic_fields__:
            default = _attributed_field_factory(
                AttributedField, cls, dyn_field
            )
            dynamic_defaults[dyn_field.get_name()] = default
        return dynamic_defaults

    def __setattr__(cls, name, value):
        if isinstance(value, Field):
            is_mapping = (
                isinstance(value, MappingField) or
                name in Document.mapping_fields
            )
            if is_mapping:
                field = value.clone(cls=MappingField)
                if field._type.__class__ == Type:
                    field._type = getattr(cls, name).get_type()
            else:
                field = value.clone()

            if field._name is None:
                field._name = name

            attr_field = AttributedField(cls, name, field)

            if is_mapping:
                cls._mapping_fields[name] = attr_field
            else:
                cls._user_fields[name] = attr_field
            cls._fields[name] = attr_field
            cls._field_name_map[field._name] = attr_field

            value = attr_field

        super(DocumentMeta, cls).__setattr__(name, value)

    @property
    def fields(cls):
        return cls._fields

    @property
    def user_fields(cls):
        return cls._user_fields

    @property
    def mapping_fields(cls):
        return cls._mapping_fields

    @property
    def dynamic_fields(cls):
        return cls._dynamic_fields

    def wildcard(cls, name):
        return DynamicAttributedField(cls, name, Field(name))

    def __getattr__(cls, name):
        return getattr(cls.fields, name)


class Document(with_metaclass(DocumentMeta)):
    __visit_name__ = 'document'

    _uid = MappingField(String)
    _id = MappingField(String)
    _type = MappingField(String)
    _source = MappingField(String)
    _all = MappingField(String)
    _analyzer = MappingField(String)
    _boost = MappingField(String)
    _parent = MappingField(String)
    _field_names = MappingField(String)
    _routing = MappingField(String)
    _index = MappingField(String)
    _size = MappingField(Integer)
    _timestamp = MappingField(Date)
    _ttl = MappingField(String)
    _version = MappingField(Integer)
    _score = MappingField(Float)

    __dynamic_fields__ = []

    __mapping_options__ = {}

    def __init__(self, _hit=None, _result=None, **kwargs):
        self._index = self._type = self._id = self._score = None
        self._hit_fields = None
        self._highlight = None
        self._matched_queries = None
        if _hit:
            self._score = _hit.get('_score')
            source = _hit.get('_source')
            fields = _hit.get('fields')
            custom_doc_type = fields.get(DOC_TYPE_NAME_FIELD) \
                if fields else None

            for attr_field in self._mapping_fields:
                setattr(self, attr_field._attr_name,
                        _hit.get(attr_field._field._name))

            if custom_doc_type:
                doc_type = custom_doc_type[0]
                _, _, self._id = _hit['_id'].rpartition(DOC_TYPE_ID_DELIMITER)
                self._type = doc_type

                custom_parent_id = fields.get(DOC_TYPE_PARENT_FIELD)
                if custom_parent_id:
                    parent_id = custom_parent_id[0]
                    _, _, self._parent = parent_id.rpartition(
                        DOC_TYPE_ID_DELIMITER
                    )

            if source:
                for hit_key, hit_value in source.items():
                    setattr(
                        self,
                        *self._process_source_key_value(hit_key, hit_value)
                    )

            if fields:
                # we cannot construct document from fields
                # in next example we cannot decide
                # which tag has name and which has not:
                # {"tags.id": [1, 2], "tags.name": ["Test"]}
                self._hit_fields = self._process_fields(fields)

            if _hit.get('highlight'):
                self._highlight = _hit['highlight']

            if _hit.get('matched_queries'):
                self._matched_queries = _hit['matched_queries']

        for fkey, fvalue in kwargs.items():
            setattr(self, fkey, fvalue)

        self._result = _result

    def _process_source_key_value(self, key, value):
        if key in self._field_name_map:
            attr_field = self._field_name_map[key]
            return (
                attr_field._attr_name,
                attr_field.get_type().to_python(value)
            )
        return key, value

    def _process_fields(self, hit_fields):
        processed_fields = {}
        for field_name, field_values in hit_fields.items():
            field_path = field_name.split('.')
            doc_cls = self.__class__
            field_type = None
            for fname in field_path:
                attr_field = doc_cls._field_name_map.get(fname)
                if not attr_field:
                    break
                field_type = attr_field.get_field().get_type()
                doc_cls = field_type.doc_cls
            if field_type:
                processed_values = list(
                    map(field_type.to_python, field_values)
                )
            else:
                processed_values = field_values
            processed_fields[field_name] = processed_values
        return processed_fields

    @classmethod
    def get_doc_type(cls):
        return getattr(cls, '__doc_type__', None)

    @classmethod
    def has_parent_doc_cls(cls):
        return hasattr(cls, '__parent__')

    @classmethod
    def get_parent_doc_cls(cls):
        return getattr(cls, '__parent__', None)

    def to_meta(self, compiler):
        meta_compiler = compiler.compiled_bulk.compiled_meta
        return meta_compiler(self).body

    def to_source(self, compiler, validate=False):
        source_compiler = compiler.compiled_bulk.compiled_source
        return source_compiler(self, validate=validate).body

    def get_highlight(self):
        return self._highlight or {}

    def get_matched_queries(self):
        return self._matched_queries or []

    def get_hit_fields(self):
        return self._hit_fields or {}

    @classmethod
    def to_mapping(cls, compiler, ordered=False):
        return compiler.compiled_put_mapping(cls, ordered=ordered).body

    @cached_property
    def instance(self):
        if self._result:
            self._result._populate_instances(self.__class__)
            return self.__dict__['instance']


class DynamicDocumentMeta(DocumentMeta):
    def _get_dynamic_defaults(cls):
        dynamic_defaults = \
            super(DynamicDocumentMeta, cls)._get_dynamic_defaults()
        if '*' not in dynamic_defaults:
            dynamic_defaults['*'] = _attributed_field_factory(
                DynamicAttributedField, cls, Field('*'))
        return dynamic_defaults

    def __getattr__(cls, name):
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError(
                "'{}' class has no attribute '{}'".format(cls, name)
            )
        return cls.fields[name]


class DynamicDocument(with_metaclass(DynamicDocumentMeta, Document)):
    def _process_source_key_value(self, key, value):
        key, value = (
            super(DynamicDocument, self)._process_source_key_value(key, value)
        )
        if isinstance(value, dict):
            return key, DynamicDocument(**value)
        return key, value
