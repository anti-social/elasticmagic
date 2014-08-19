from .types import String
from .expression import Field
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
    def __init__(self, _hit=None, **kwargs):
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

    def to_dict(self):
        dct = {}
        for field in self._fields:
            dct[field._attr_name] = field._to_dict(getattr(self, field._attr_name, None))
        return dct
