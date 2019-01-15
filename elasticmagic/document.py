from .types import Type, String, Integer, Float, Date, ValidationError
from .compiler import DefaultCompiler
from .attribute import AttributedField, DynamicAttributedField
from .attribute import _attributed_field_factory
from .expression import Field, MappingField
from .datastructures import OrderedAttributes
from .util import cached_property
from .compat import with_metaclass


META_FIELD_NAMES = {
    '_id',
    '_index',
    '_type',
    '_routing',
    '_parent',
    '_timestamp',
    '_ttl',
    '_version',
}


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
            for attr_field in self._mapping_fields:
                setattr(self, attr_field._attr_name,
                        _hit.get(attr_field._field._name))
            if _hit.get('_source'):
                for hit_key, hit_value in _hit['_source'].items():
                    setattr(
                        self,
                        *self._process_source_key_value(hit_key, hit_value)
                    )
            if _hit.get('fields'):
                # we cannot construct document from fields
                # in next example we cannot decide
                # which tag has name and which has not:
                # {"tags.id": [1, 2], "tags.name": ["Test"]}
                self._hit_fields = self._process_fields(_hit['fields'])
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
            return attr_field._attr_name, attr_field._to_python(value)
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

    def to_meta(self):
        doc_meta = {}
        if hasattr(self, '__doc_type__'):
            doc_meta['_type'] = self.__doc_type__
        for field_name in META_FIELD_NAMES:
            value = getattr(self, field_name, None)
            if value:
                doc_meta[field_name] = value
        return doc_meta

    def to_source(self, validate=False):
        res = {}
        for key, value in self.__dict__.items():
            if key in self.__class__.mapping_fields:
                continue

            attr_field = self.__class__.fields.get(key)
            if attr_field:
                if value is None or value == '' or value == []:
                    if (
                        validate and
                        attr_field.get_field()._mapping_options.get('required')
                    ):
                        raise ValidationError("'{}' is required".format(
                            attr_field.get_attr_name()
                        ))
                    continue
                value = attr_field.get_type() \
                    .from_python(value, validate=validate)
                res[attr_field._field._name] = value

        for attr_field in self._fields.values():
            if (
                validate
                and attr_field.get_field()._mapping_options.get('required')
                and attr_field.get_field().get_name() not in res
            ):
                raise ValidationError(
                    "'{}' is required".format(attr_field.get_attr_name())
                )

        return res

    def get_highlight(self):
        return self._highlight or {}

    def get_matched_queries(self):
        return self._matched_queries or []

    def get_hit_fields(self):
        return self._hit_fields or {}

    @classmethod
    def to_mapping(cls, compiler=None, ordered=False):
        mapping_compiler = (compiler or DefaultCompiler).compiled_mapping
        return mapping_compiler(cls, ordered=ordered).params

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
