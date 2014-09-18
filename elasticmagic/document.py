import fnmatch

from .types import String
from .expression import Field, Fields
from .util import cached_property
from .compat import with_metaclass


class DocumentMeta(type):
    def __new__(meta, name, bases, dct):
        cls = type.__new__(meta, name, bases, dct)

        cls._fields = []
        cls._fields_map = {}

        if not hasattr(cls, '_id'):
            cls._id = Field(String())

        for field_name in dir(cls):
            # _id doesn't indexed, so do not add it in _fields
            if field_name == '_id':
                continue

            field = getattr(cls, field_name)
            if isinstance(field, Field):
                field._bind(cls, field_name)
                cls._fields.append(field)
                cls._fields_map[field_name] = field

        return cls

    def __getattr__(cls, name):
        field = cls._from_dynamic_field(name)
        if field:
            # setattr(cls, name, field)
            return field
        raise AttributeError("'%s' document has no field '%s'" % (cls.__name__, name))

    def _from_dynamic_field(cls, name):
        for dyn_field in cls.__dynamic_fields__:
            if fnmatch.fnmatch(name, dyn_field._name):
                field = Field(name, dyn_field._type)
                field._bind(cls, name)
                return field


class Document(with_metaclass(DocumentMeta)):
    __dynamic_fields__ = []

    def __init__(self, _hit=None, _result=None, **kwargs):
        self._index = self._type = self._id = self._score = None
        if _hit:
            self._index = _hit.get('_index')
            self._type = _hit.get('_type')
            self._id = _hit.get('_id')
            self._score = _hit.get('_score')
            source = _hit.get('_source')
            if source:
                for field in self._fields:
                    setattr(self, field._attr_name, field._to_python(source.get(field._name)))

        for fkey, fvalue in kwargs.items():
            setattr(self, fkey, fvalue)

        self._result = _result

    def to_dict(self):
        res = {}
        for key, value in self.__dict__.items():
            if value is None or value == '' or value == []:
                continue

            field = None
            if key in self._fields_map:
                field = self._fields_map[key]
            else:
                field = self.__class__._from_dynamic_field(key)

            if field:
                res[field._attr_name] = field._to_dict(value)

        return res

    @classmethod
    def instance_mapper(self):
        return {}

    @cached_property
    def instance(self):
        if self._result:
            self._result._populate_instances()
            return self.__dict__['instance']


class DynamicDocumentMeta(DocumentMeta):
    @property
    def fields(cls):
        return Fields(cls)

    f = fields

    def __getattr__(cls, name):
        field = Field(name)
        field._bind(cls, name)
        return field


class DynamicDocument(with_metaclass(DynamicDocumentMeta, Document)):
    pass
