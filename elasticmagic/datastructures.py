import fnmatch


class OrderedAttributes(object):
    __visit_name__ = 'ordered_attributes'

    def __init__(self, data=None, defaults=None):
        self._dict = {}
        self._keys = []
        self._defaults = defaults or {}

        if data:
            for k, v in data:
                self[k] = v

    def _get_default(self, key):
        for template, default in self._defaults.items():
            if fnmatch.fnmatch(key, template):
                return default

    def __setitem__(self, key, value):
        self._dict[key] = value
        self._keys.append(key)

    def __getitem__(self, key):
        if key not in self._dict:
            default = self._get_default(key)
            if default:
                return default(key)
        return self._dict[key]

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("Has no field '%s'" % key)

    def __contains__(self, key):
        return key in self._dict

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return iter(self._keys)

    def values(self):
        return (self._dict[k] for k in self._keys)

    def items(self):
        return ((k, self._dict[k]) for k in self._keys)

    def __iter__(self):
        return self.values()

    def __len__(self):
        return len(self._dict)
