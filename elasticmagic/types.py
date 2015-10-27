import base64
import datetime
import inspect
import re

import dateutil.parser

try:
    import geohash
    GEOHASH_IMPORTED = True
except ImportError:
    GEOHASH_IMPORTED = False

from .compat import text_type, string_types


def instantiate(typeobj, *args, **kwargs):
    if inspect.isclass(typeobj):
        return typeobj(*args, **kwargs)
    return typeobj


class ValidationError(ValueError):
    pass


class Type(object):
    def __init__(self):
        self.doc_cls = None

    def to_python(self, value):
        if value is None:
            return None
        return value

    def to_python_single(self, value):
        return self.to_python(value)

    def from_python(self, value, validate=False):
        return value


class String(Type):
    __visit_name__ = 'string'

    def to_python(self, value):
        if value is None:
            return None
        return text_type(value)

    def from_python(self, value,  validate=False):
        return text_type(value)


class _Int(Type):
    def to_python(self, value):
        if value is None:
            return None
        return int(value)

    def from_python(self, value, validate=False):
        if validate:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise ValidationError(
                    "Cannot parse value as integer: {!r}".format(value)
                )

            if value < self.MIN_VALUE or value > self.MAX_VALUE:
                raise ValidationError(
                    'Value must be in range: {} not in [{}, {}]'.format(
                        value, self.MIN_VALUE, self.MAX_VALUE
                    )
                )
        return value


class Byte(_Int):
    __visit_name__ = 'byte'

    MIN_VALUE = -(1 << 7)
    MAX_VALUE = (1 << 7) - 1


class Short(_Int):
    __visit_name__ = 'short'

    MIN_VALUE = -(1 << 15)
    MAX_VALUE = (1 << 15) - 1


class Integer(_Int):
    __visit_name__ = 'integer'

    MIN_VALUE = -(1 << 31)
    MAX_VALUE = (1 << 31) - 1


class Long(_Int):
    __visit_name__ = 'long'

    MIN_VALUE = -(1 << 63)
    MAX_VALUE = (1 << 63) - 1


class _Float(Type):
    def to_python(self, value):
        if value is None:
            return None
        return float(value)

    def from_python(self, value, validate=False):
        if validate:
            try:
                value = float(value)
            except (ValueError, TypeError):
                raise ValidationError(
                    "Cannot parse value as integer: {!r}".format(value)
                )
        return value


class Float(_Float):
    __visit_name__ = 'float'


class Double(_Float):
    __visit_name__ = 'double'


class Date(Type):
    __visit_name__ = 'date'

    # def __init__(self, format=None):
    #     self.format = format

    def to_python(self, value):
        if value is None:
            return None
        return dateutil.parser.parse(value)

    def from_python(self, value, validate=True):
        if validate:
            if not isinstance(value, datetime.datetime):
                raise ValidationError('Value must be datetime.datetime object')
        return value


class Boolean(Type):
    __visit_name__ = 'boolean'

    def to_python(self, value):
        if value is None:
            return None
        if value is False or value == 0 or value in ('', 'false', 'F'):
            return False
        return True

    def from_python(self, value, validate=True):
        return bool(value)


class Binary(Type):
    __visit_name__ = 'binary'

    def to_python(self, value):
        if value is None:
            return None
        return base64.b64decode(value)

    def from_python(self, value, validate=False):
        try:
            return base64.b64encode(value).decode()
        except (ValueError, TypeError):
            if validate:
                raise ValidationError(
                    'Cannot decode value from base64: {!r}'.format(value)
                )
            else:
                raise


class Ip(Type):
    __visit_name__ = 'ip'

    IPV4_REGEXP = re.compile(
        r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    )

    def from_python(self, value, validate=False):
        if validate:
            try:
                if not self.IPV4_REGEXP.match(value):
                    raise ValidationError('Not valid IPv4 address: {}'.format(value))
            except (TypeError, ValueError) as e:
                raise ValidationError(*e.args)
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

    def from_python(self, value, validate=False):
        if isinstance(value, self.doc_cls):
            return value.to_source(validate=validate)
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

    def from_python(self, value, validate=False):
        if not isinstance(value, list):
            value = [value]
        return [self.sub_type.from_python(v, validate=validate) for v in value]


class GeoPoint(Type):
    __visit_name__ = 'geo_point'

    LAT_LON_SEPARATOR = ','

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            value = list(reversed(value))
        if isinstance(value, string_types):
            if self.LAT_LON_SEPARATOR in value:
                value = list(value.split(self.LAT_LON_SEPARATOR))
            elif GEOHASH_IMPORTED:
                value = list(geohash.decode(value))
        elif isinstance(value, dict):
            value = [value.get('lat'), value.get('lon')]
        return {'lat': float(value[0]), 'lon': float(value[1])}
    
    def from_python(self, value, validate=False):
        if validate:
            if not isinstance(value, dict):
                raise ValidationError('Value must be dictionary: {!r}'.format(value))
            if len(value) != 2 or 'lat' not in value or 'lon' not in value:
                raise ValidationError(
                    "Only 'lat' and 'lon' keys must present in the dictionary"
                )
            try:
                value = {'lat': float(value['lat']), 'lon': float(value['lon'])}
            except (ValueError, TypeError):
                raise ValidationError('Lat/lon must be floats: {!r}', value)
        return value
