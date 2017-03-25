import math
from itertools import starmap
from functools import partial
from collections import defaultdict

from elasticmagic.types import Float, Integer, Long, instantiate
from elasticmagic.compat import force_unicode, zip_longest


def to_float(value, type=None):
    type = type or Float()
    v = type.to_python_single(value)
    if math.isnan(v) or math.isinf(v):
        raise ValueError('NaN or Inf is not supported')
    return v


def to_int(value, type=None):
    type = type or Integer()
    v = type.to_python_single(value)
    if Integer.MIN_VALUE < v < Integer.MAX_VALUE:
        return v
    raise ValueError(
        'Integer value must be between %s and %s' % (
            Integer.MIN_VALUE, Integer.MAX_VALUE
        )
    )


def to_long(value, type=None):
    type = type or Long()
    v = type.to_python_single(value)
    if Long.MIN_VALUE < v < Long.MAX_VALUE:
        return v
    raise ValueError(
        'Long value must be between %s and %s' % (
            Long.MIN_VALUE, Long.MAX_VALUE
        )
    )


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
    VALUES_SEP = ':'

    NULL_VAL = 'null'
    TRUE_VAL = 'true'
    FALSE_VAL = 'false'

    DEFAULT_OP = 'exact'

    PROCESSOR_FACTORIES = {
        Float: lambda type: partial(to_float, type=type),
        Integer: lambda type: partial(to_int, type=type),
        Long: lambda type: partial(to_long, type=type),
    }

    def _normalize_params(self, params):
        if hasattr(params, 'dict_of_lists'):
            # Webob's MultiDict
            return params.dict_of_lists()
        if hasattr(params, 'lists'):
            # Django's QueryDict
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

        raise TypeError("'params' must be Webob's MultiDict, "
                        "Django's QueryDict, list, tuple or dict")

    def decode_value(self, value, typelist=None):
        typelist = [instantiate(t) for t in wrap_list(typelist or [])]
        raw_values = force_unicode(value).split(self.VALUES_SEP)

        decoded_values = []
        for v, type in zip_longest(raw_values, typelist):
            if type is None:
                to_python = force_unicode
            else:
                to_python_factory = self.PROCESSOR_FACTORIES.get(type.__class__)
                if to_python_factory:
                    to_python = to_python_factory(type)
                else:
                    to_python = type.to_python_single
            if v is None:
                continue

            if v == self.NULL_VAL:
                decoded_values.append(None)
            else:
                try:
                    decoded_values.append(to_python(v))
                except ValueError:
                    break

        return decoded_values

    def decode(self, params, types=None):
        params = self._normalize_params(params)
        types = types or {}
        decoded_params = {}
        for name, v in params.items():
            name, _, op = name.partition(self.OP_SEP)
            if not op:
                op = self.DEFAULT_OP
            for w in wrap_list(v):
                decoded_values = self.decode_value(w, types.get(name))
                if decoded_values:
                    decoded_params \
                        .setdefault(name, {}) \
                        .setdefault(op, []) \
                        .append(decoded_values)

        return decoded_params

    def _encode_value(self, value, type):
        if value is None:
            return self.NULL_VAL
        if value is True:
            return self.TRUE_VAL
        if value is False:
            return self.FALSE_VAL
        if type:
            value = type.from_python(value, validate=True)
        return force_unicode(value)

    def encode_value(self, value, typelist=None):
        typelist = [instantiate(t) for t in wrap_list(typelist or [])]
        return self.VALUES_SEP.join(
            self._encode_value(v, t)
            for v, t in zip_longest(wrap_list(value), typelist)
        )

    def encode(self, values, types=None):
        params = {}
        for name, ops in values.items():
            for op, vals in ops.items():
                if op == self.DEFAULT_OP:
                    key = name
                else:
                    key = '{}__{}'.format(name, op)
                if types:
                    typelist = types.get(name)
                else:
                    typelist = None
                params[key] = [
                    self.encode_value(v, typelist=typelist)
                    for v in vals
                ]
        return params
