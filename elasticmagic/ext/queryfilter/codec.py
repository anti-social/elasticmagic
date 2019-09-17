import datetime
import math
from collections import defaultdict

import dateutil.parser

from elasticmagic.types import instantiate
from elasticmagic.types import Type
from elasticmagic.compat import force_unicode
from elasticmagic.compat import int_types


TIME_ATTRS = {'hour', 'minute', 'second', 'microsecond', 'tzinfo'}


class TypeCodec(object):
    def decode(self, value, es_type=None):
        raise NotImplementedError

    def encode(self, value, es_type=None):
        raise NotImplementedError


class StringCodec(TypeCodec):
    def decode(self, value, es_type=None):
        return force_unicode(value)

    def encode(self, value, es_type=None):
        return force_unicode(value)


class FloatCodec(TypeCodec):
    def decode(self, value, es_type=None):
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            raise ValueError('NaN or Inf is not supported')
        return v

    def encode(self, value, es_type=None):
        return value


class IntCodec(TypeCodec):
    def encode(self, value, es_type=None):
        if isinstance(value, int_types):
            return force_unicode(value)
        return force_unicode(int(value))

    def decode(self, value, es_type=None):
        v = int(value)
        if (
                es_type is not None and
                (v < es_type.MIN_VALUE or v > es_type.MAX_VALUE)
        ):
            raise ValueError(
                'Value must be between {} and {}'.format(
                    es_type.MIN_VALUE, es_type.MAX_VALUE
                )
            )
        return v


class BoolCodec(TypeCodec):
    def encode(self, value, es_type=None):
        if value is True:
            return 'true'
        if value is False:
            return 'false'
        return bool(value)

    def decode(self, value, es_type=None):
        if isinstance(value, bool):
            return value
        if value == 'true':
            return True
        if value == 'false':
            return False
        raise ValueError('Cannot decode boolean value: {}'.format(value))


class DateCodec(TypeCodec):
    def encode(self, value, es_type=None):
        if isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%dT%H:%M:%S.%f')
        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        raise ValueError('Value must be date or datetime: {}'.format(value))

    def decode(self, value, es_type=None):
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value
        return dateutil.parser.parse(value)


def wrap_list(v):
    if not isinstance(v, (list, tuple)):
        return [v]
    return v


class BaseCodec(object):
    def decode_value(self, value, es_type=None):
        raise NotImplementedError()

    def decode(self, params, types=None):
        raise NotImplementedError()

    def encode_value(self, value, es_type=None):
        raise NotImplementedError()

    def encode(self, values, types=None):
        raise NotImplementedError()


class SimpleCodec(BaseCodec):
    OP_SEP = '__'

    NULL_VAL = 'null'

    DEFAULT_OP = 'exact'

    CODECS = {
        None: StringCodec,
        float: FloatCodec,
        int: IntCodec,
        bool: BoolCodec,
        datetime.datetime: DateCodec,
    }

    @staticmethod
    def _normalize_params(params):
        if hasattr(params, 'getall'):
            # Webob
            return params.dict_of_lists()
        if hasattr(params, 'getlist'):
            # Django
            return dict(params.lists())
        if isinstance(params, (list, tuple)):
            # list, tuple
            new_params = defaultdict(list)
            for p, v in params:
                new_params[p].extend(v)
            return new_params
        if isinstance(params, dict):
            # dict
            return params

        raise TypeError("'params' must be Webob MultiDict, "
                        "Django QueryDict, list, tuple or dict")

    @staticmethod
    def _get_es_type_class(es_type):
        if es_type is not None and isinstance(es_type, Type):
            if es_type.sub_type:
                return SimpleCodec._get_es_type_class(es_type.sub_type)
            return es_type.__class__
        return es_type

    @staticmethod
    def _get_es_and_python_types(es_type):
        if es_type is None:
            return None, None
        es_type = instantiate(es_type)
        if es_type.sub_type:
            es_type = es_type.sub_type
        return es_type, es_type.python_type

    def decode_value(self, value, es_type=None):
        if value is None or value == self.NULL_VAL:
            return None

        es_type, python_type = self._get_es_and_python_types(es_type)
        value_codec = self.CODECS.get(python_type, StringCodec)()
        return value_codec.decode(value, es_type=es_type)

    def decode(self, params, types=None):
        params = self._normalize_params(params)
        types = types or {}
        decoded_params = {}
        for name, v in params.items():
            name, _, op = name.partition(self.OP_SEP)
            if not op:
                op = self.DEFAULT_OP
            es_type = types.get(name)
            for w in wrap_list(v):
                try:
                    decoded_value = self.decode_value(w, es_type=es_type)
                    decoded_params \
                        .setdefault(name, {}) \
                        .setdefault(op, []) \
                        .append(decoded_value)
                except ValueError:
                    # just ignore values we cannot decode
                    pass

        return decoded_params

    def encode_value(self, value, es_type=None):
        if value is None:
            return self.NULL_VAL

        es_type, python_type = self._get_es_and_python_types(es_type)
        value_codec = self.CODECS.get(python_type, StringCodec)()
        return value_codec.encode(value, es_type=es_type)

    def encode(self, values, types=None):
        params = {}
        for name, ops in values.items():
            for op, vals in ops.items():
                if op == self.DEFAULT_OP:
                    key = name
                else:
                    key = '{}__{}'.format(name, op)
                if types:
                    es_type = types.get(name)
                else:
                    es_type = None
                params[key] = [
                    self.encode_value(v, es_type=es_type)
                    for v in vals
                ]
        return params
