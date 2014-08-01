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
