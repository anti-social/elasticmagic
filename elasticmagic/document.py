from .types import String
from .expression import Field
from .compat import with_metaclass


class DocumentMeta(type):
    def __new__(meta, name, bases, dct):
        cls = type.__new__(meta, name, bases, dct)

        if not hasattr(cls, '_id'):
            cls._id = Field(String())

        for field_name, field in cls.__dict__.items():
            if isinstance(field, Field):
                field.name = field_name
                field.doc_cls = cls

        return cls


class Document(with_metaclass(DocumentMeta)):
    def __init__(self, _hit=None, **kwargs):
        self._index = self._type = self._id = self._score = None
        if _hit:
            self._index = _hit['_index']
            self._type = _hit['_type']
            self._id = _hit['_id']
            self._score = _hit['_score']
            for skey, svalue in _hit['_source'].items():
                field = getattr(self.__class__, skey, None)
                if field:
                    svalue = field.type.to_python(svalue)
                setattr(self, skey, svalue)

        for fkey, fvalue in kwargs.items():
            setattr(self, fkey, fvalue)
