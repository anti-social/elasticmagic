import math
from itertools import starmap
from functools import partial
from collections import defaultdict

from elasticmagic.types import Float, Integer, Long, instantiate
from elasticmagic.compat import force_unicode, zip_longest


def to_float(value, type=None):
    type = type or Float()
    v = type.to_python(value)
    if math.isnan(v) or math.isinf(v):
        raise ValueError('NaN or Inf is not supported')
    return v


def to_int(value, type=None):
    type = type or Integer()
    v = type.to_python(value)
    if Integer.MIN_VALUE < v < Integer.MAX_VALUE:
        return v
    raise ValueError(
        'Integer value must be between %s and %s' % (
            Integer.MIN_VALUE, Integer.MAX_VALUE
        )
    )


def to_long(value, type=None):
    type = type or Long()
    v = type.to_python(value)
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
    VALUES_SEP = ';'
    RANGE_SEP = ':'

    NULL_VAL = 'null'
    TRUE_VAL = 'true'
    FALSE_VAL = 'false'

    PROCESSOR_FACTORIES = {
        Float: lambda type: partial(to_float, type=type),
        Integer: lambda type: partial(to_int, type=type),
        Long: lambda type: partial(to_long, type=type),
    }

    def _normalize_params(self, params):
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
                    to_python = type.to_python
            if v is None:
                continue
            try:
                if v == self.NULL_VAL:
                    decoded_values.append(None)
                else:
                    vals = v.split(self.RANGE_SEP)
                    if len(vals) > 1:
                        decoded_values.append(tuple((to_python(w) if w else None) for w in vals))
                    else:
                        decoded_values.append(to_python(v))
            except ValueError:
                pass
        return decoded_values
        
    
    def decode(self, params, types=None):
        params = self._normalize_params(params)
        types = types or {}
        data = defaultdict(list)
        for name, v in params.items():
            for w in wrap_list(v):
                decoded_values = self.decode_value(w, types.get(name))
                if decoded_values:
                    data[name].append(decoded_values)

        return dict(data)

    def _encode_value(self, value):
        if value is None:
            return self.NULL_VAL
        if value is True:
            return self.TRUE_VAL
        if value is False:
            return self.FALSE_VAL
        return force_unicode(value)
        
    def encode_value(self, value, typelist=None):
        return self.VALUES_SEP.join(self._encode_value(v) for v in wrap_list(value))

    def encode(self, values, types=None):
        params = defaultdict(list)
        for name, value in values:
            params[name].append(self.encode_value(value))
        return dict(params)
