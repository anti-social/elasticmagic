import collections
from functools import wraps
from itertools import chain


def _with_clone(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        clone = self.clone()
        res = fn(clone, *args, **kwargs)
        if res is not None:
            return res
        return clone
    return wrapper


class cached_property(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self

        res = instance.__dict__[self.func.__name__] = self.func(instance)
        return res


def to_camel_case(s):
    return u''.join(map(lambda w: w.capitalize(), s.split('_')))


def clean_params(params, **kwargs):
    return {
        p: v for p, v in chain(params.items(), kwargs.items())
        if v is not None
    }


def collect_doc_classes(expr):
    if hasattr(expr, '_collect_doc_classes'):
        return expr._collect_doc_classes()

    if isinstance(expr, dict):
        return set().union(
            *[collect_doc_classes(e)
              for e in chain(expr.keys(), expr.values())]
        )

    if isinstance(expr, (list, tuple)):
        return set().union(*[collect_doc_classes(e) for e in expr])

    return set()


def maybe_float(value):
    if value is None:
        return None
    return float(value)


def merge_params(params, args, kwargs):
    assert isinstance(args, collections.Iterable), args
    assert isinstance(kwargs, collections.Mapping), kwargs
    new = dict()
    for a in args:
        new.update(a)
    new.update(kwargs)
    return type(params)(params, **new)
