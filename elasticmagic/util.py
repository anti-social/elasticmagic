from string import capitalize
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


def to_camel_case(s):
    return u''.join(map(capitalize, s.split('_')))
