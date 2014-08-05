from .types import String
from .expression import Field
from .compat import with_metaclass


class DocumentMeta(type):
    def __new__(meta, name, bases, dct):
        cls = type.__new__(meta, name, bases, dct)

        if not hasattr(cls, 'id'):
            cls.id = Field(String())

        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, Field):
                attr_value.name = attr_name
                attr_value.doc_cls = cls

        return cls


class Document(with_metaclass(DocumentMeta)):
    def __init__(self, **kwargs):
        for fname, fvalue in kwargs.items():
            setattr(self, fname, fvalue)
