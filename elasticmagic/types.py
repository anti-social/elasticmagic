import inspect

import dateutil.parser

from .compat import text_type


def instantiate(typeobj, *args, **kwargs):
    if inspect.isclass(typeobj):
        return typeobj(*args, **kwargs)
    return typeobj


class Type(object):
    def to_python(self, value):
        return value

    def to_dict(self, value):
        return value

    def has_sub_fields(self):
        return False

    def sub_field(self, prefix, name, doc_cls):
        raise NotImplementedError()


class String(Type):
    def to_python(self, value):
        if value is None:
            return None
        return text_type(value)


class Byte(Type):
    MIN_VALUE = -(1 << 7)
    MAX_VALUE = (1 << 7) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Short(Type):
    MIN_VALUE = -(1 << 15)
    MAX_VALUE = (1 << 15) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Integer(Type):
    MIN_VALUE = -(1 << 31)
    MAX_VALUE = (1 << 31) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Long(Type):
    MIN_VALUE = -(1 << 63)
    MAX_VALUE = (1 << 63) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Float(Type):
    def to_python(self, value):
        if value is None:
            return None
        return float(value)


class Double(Type):
    def to_python(self, value):
        if value is None:
            return None
        return float(value)


class Date(Type):
    # def __init__(self, format=None):
    #     self.format = format

    def to_python(self, value):
        if value is None:
            return None
        return dateutil.parser.parse(value)


class Boolean(Type):
    def to_python(self, value):
        if value is None:
            return None
        elif value is False or value == 0 or value in ('false', 'F'):
            return False
        return True


class Binary(Type):
    def to_python(self, value):
        # TODO: decode base64
        return value


class Ip(Type):
    def to_python(self, value):
        return value


class Object(Type):
    def __init__(self, doc_cls):
        self.doc_cls = doc_cls

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, self.doc_cls):
            return value
        return self.doc_cls(**value)

    def to_dict(self, value):
        if value is None:
            return None
        if isinstance(value, self.doc_cls):
            return value.to_dict()
        return value

    def has_sub_fields(self):
        return True

    def sub_field(self, prefix, name, doc_cls):
        from .expression import Field

        full_name = '{}.{}'.format(prefix, name)
        return Field(full_name, getattr(self.doc_cls, name)._type,
                     _doc_cls=doc_cls,
                     _attr_name=full_name)


class Nested(Object):
    pass


class MultiField(Type):
    def __init__(self, main_type, fields):
        self.main_type = instantiate(main_type)
        self.fields = fields

    def to_python(self, value):
        if value is None:
            return None
        return self.main_type.to_python(value)

    def has_sub_fields(self):
        return True

    def sub_field(self, prefix, name, doc_cls):
        from .expression import Field

        full_name = '{}.{}'.format(prefix, name)
        return Field(full_name, instantiate(self.fields[name]),
                     _doc_cls=doc_cls,
                     _attr_name=full_name)


class List(Type):
    def __init__(self, sub_type):
        self.sub_type = instantiate(sub_type)

    def to_python(self, value):
        if value is None:
            return None
        if not isinstance(value, list):
            value = [value]
        return [self.sub_type.to_python(v) for v in value]

    def to_dict(self, value):
        if value is None:
            return None
        if not isinstance(value, list):
            value = [value]
        return [self.sub_type.to_dict(v) for v in value]

    def has_sub_fields(self):
        return self.sub_type.has_sub_fields()

    def sub_field(self, prefix, name, doc_cls):
        return self.sub_type.sub_field(prefix, name, doc_cls)
