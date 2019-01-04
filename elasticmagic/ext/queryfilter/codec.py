import datetime
import math
from functools import partial
from collections import defaultdict

from elasticmagic.types import Date, Float, Integer, Long, instantiate
from elasticmagic.compat import force_unicode


TIME_ATTRS = {'hour', 'minute', 'second', 'microsecond', 'tzinfo'}


def to_float(value, es_type=None):
    es_type = es_type or Float()
    v = es_type.to_python_single(value)
    if math.isnan(v) or math.isinf(v):
        raise ValueError('NaN or Inf is not supported')
    return v


def to_int(value, es_type=None):
    es_type = es_type or Integer()
    v = es_type.to_python_single(value)
    if Integer.MIN_VALUE < v < Integer.MAX_VALUE:
        return v
    raise ValueError(
        'Integer value must be between %s and %s' % (
            Integer.MIN_VALUE, Integer.MAX_VALUE
        )
    )


def to_long(value, es_type=None):
    es_type = es_type or Long()
    v = es_type.to_python_single(value)
    if Long.MIN_VALUE < v < Long.MAX_VALUE:
        return v
    raise ValueError(
        'Long value must be between %s and %s' % (
            Long.MIN_VALUE, Long.MAX_VALUE
        )
    )


def to_date(value, es_type=None):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value
    es_type = es_type or Date()
    return es_type.to_python_single(value)


def wrap_list(v):
    if not isinstance(v, (list, tuple)):
        return [v]
    return v


class BaseCodec(object):
    def decode_value(self, value, typelist=None):
        raise NotImplementedError()

    def decode(self, params, types=None):
        raise NotImplementedError()

    def encode_value(self, value, typelist=None):
        raise NotImplementedError()

    def encode(self, values, types=None):
        raise NotImplementedError()


class SimpleCodec(BaseCodec):
    OP_SEP = '__'

    NULL_VAL = 'null'
    TRUE_VAL = 'true'
    FALSE_VAL = 'false'

    DEFAULT_OP = 'exact'

    PROCESSOR_FACTORIES = {
        Float: lambda t: partial(to_float, es_type=t),
        Integer: lambda t: partial(to_int, es_type=t),
        Long: lambda t: partial(to_long, es_type=t),
        Date: lambda t: partial(to_date, es_type=t),
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

    def decode_value(self, value, es_type=None):
        es_type = instantiate(es_type)

        if es_type is None:
            to_python = force_unicode
        else:
            to_python_factory = self.PROCESSOR_FACTORIES.get(
                es_type.__class__
            )
            if to_python_factory:
                to_python = to_python_factory(es_type)
            else:
                to_python = es_type.to_python_single

        if value is None or value == self.NULL_VAL:
            return None
        else:
            return to_python(value)

    def decode(self, params, types=None):
        params = self._normalize_params(params)
        types = types or {}
        decoded_params = {}
        for name, v in params.items():
            name, _, op = name.partition(self.OP_SEP)
            if not op:
                op = self.DEFAULT_OP
            for w in wrap_list(v):
                try:
                    decoded_value = self.decode_value(
                        w, es_type=types.get(name)
                    )
                    decoded_params \
                        .setdefault(name, {}) \
                        .setdefault(op, []) \
                        .append(decoded_value)
                except ValueError:
                    pass

        return decoded_params

    def _encode_value(self, value, es_type=None):
        if value is None:
            return self.NULL_VAL
        if value is True:
            return self.TRUE_VAL
        if value is False:
            return self.FALSE_VAL
        if es_type:
            value = es_type.from_python(value, validate=True)
        return force_unicode(value)

    def encode_value(self, value, es_type=None):
        es_type = instantiate(es_type)
        return self._encode_value(value, es_type)

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
