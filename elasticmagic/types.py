import inspect

import dateutil.parser

from .compat import text_type


def instantiate(typeobj, *args, **kwargs):
    if inspect.isclass(typeobj):
        return typeobj(*args, **kwargs)
    return typeobj


class Type(object):
    def __init__(self):
        self.doc_cls = None

    def to_python(self, value):
        return value

    def to_python_single(self, value):
        return self.to_python(value)

    def from_python(self, value):
        return value


class String(Type):
    __visit_name__ = 'string'

    def to_python(self, value):
        if value is None:
            return None
        return text_type(value)


class Byte(Type):
    __visit_name__ = 'byte'

    MIN_VALUE = -(1 << 7)
    MAX_VALUE = (1 << 7) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Short(Type):
    __visit_name__ = 'short'

    MIN_VALUE = -(1 << 15)
    MAX_VALUE = (1 << 15) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Integer(Type):
    __visit_name__ = 'integer'

    MIN_VALUE = -(1 << 31)
    MAX_VALUE = (1 << 31) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Long(Type):
    __visit_name__ = 'long'

    MIN_VALUE = -(1 << 63)
    MAX_VALUE = (1 << 63) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Float(Type):
    __visit_name__ = 'float'

    def to_python(self, value):
        if value is None:
            return None
        return float(value)


class Double(Type):
    __visit_name__ = 'double'

    def to_python(self, value):
        if value is None:
            return None
        return float(value)


class Date(Type):
    __visit_name__ = 'date'

    # def __init__(self, format=None):
    #     self.format = format

    def to_python(self, value):
        if value is None:
            return None
        return dateutil.parser.parse(value)


class Boolean(Type):
    __visit_name__ = 'boolean'

    def to_python(self, value):
        if value is None:
            return None
        elif value is False or value == 0 or value in ('false', 'F'):
            return False
        return True


class Binary(Type):
    __visit_name__ = 'binary'

    def to_python(self, value):
        # TODO: decode base64
        return value


class Ip(Type):
    __visit_name__ = 'ip'

    def to_python(self, value):
        return value


class Object(Type):
    __visit_name__ = 'object'

    def __init__(self, doc_cls):
        self.doc_cls = doc_cls

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, self.doc_cls):
            return value
        return self.doc_cls(**value)

    def from_python(self, value):
        if value is None:
            return None
        if isinstance(value, self.doc_cls):
            return value.to_source()
        return value


class Nested(Object):
    __visit_name__ = 'nested'


class List(Type):
    def __init__(self, sub_type):
        self.sub_type = instantiate(sub_type)

    @property
    def __visit_name__(self):
        return self.sub_type.__visit_name__

    @property
    def doc_cls(self):
        return self.sub_type.doc_cls

    def to_python(self, value):
        if value is None:
            return None
        if not isinstance(value, list):
            value = [value]
        return [self.sub_type.to_python(v) for v in value]

    def to_python_single(self, value):
        v = self.to_python(value)
        if v:
            return v[0]

    def from_python(self, value):
        if value is None:
            return None
        if not isinstance(value, list):
            value = [value]
        return [self.sub_type.from_python(v) for v in value]


class GeoPoint(Type):
    __visit_name__ = 'geo_point'

    def __init__(self):
        self.doc_cls = None
        self.value_type = Float

    def from_python(self, value):
        if value is None:
            return None
        if isinstance(value, text_type):
            value = value.split(',')
        elif isinstance(value, dict):
            value = [value.get('lat'), value.get('lon')]
        if not isinstance(value, list):
            return None
        return [self.value_type.from_python(v) for v in value]
