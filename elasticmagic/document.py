from .types import String
from .expression import Field, Fields
from .util import cached_property
from .compat import with_metaclass


class DocumentMeta(type):
    def __new__(meta, name, bases, dct):
        cls = type.__new__(meta, name, bases, dct)

        cls._fields = []

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

        return cls


class Document(with_metaclass(DocumentMeta)):
    def __init__(self, _hit=None, _result=None, **kwargs):
        self._index = self._type = self._id = self._score = None
        if _hit:
            self._index = _hit.get('_index')
            self._type = _hit.get('_type')
            self._id = _hit.get('_id')
            self._score = _hit.get('_score')
            source = _hit['_source']
            for field in self._fields:
                setattr(self, field._attr_name, field._to_python(source.get(field._name)))

        for fkey, fvalue in kwargs.items():
            setattr(self, fkey, fvalue)

        self._result = _result

    def to_dict(self):
        dct = {}
        for field in self._fields:
            dct[field._attr_name] = field._to_dict(getattr(self, field._attr_name, None))
        return dct

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
