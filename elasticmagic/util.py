from functools import wraps


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


def clean_params(params):
    return {p: v for p, v in params.items() if v is not None}
