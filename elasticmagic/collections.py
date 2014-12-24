class OrderedAttributes(object):
    def __init__(self, data=None):
        self._dict = {}
        self._keys = []

        if data:
            for k, v in data:
                self[k] = v

    def __setitem__(self, key, value):
        self._dict[key] = value
        self._keys.append(key)

    def __getitem__(self, key):
        return self._dict[key]

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("Has no field '%s'" % key)

    def __contains__(self, key):
        return self.has_key(key)

    def has_key(self, key):
        return self._dict.has_key(key)

    def get(self, key, default=None):
        return self._dict.get(key, default)

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


class DynamicOrderedAttributes(OrderedAttributes):
    def __init__(self, data=None, default=None):
        super(DynamicOrderedAttributes, self).__init__(data)
        self._default = default

    def __getitem__(self, key):
        if self.has_key(key):
            return super(DynamicOrderedAttributes, self).__getitem__(key)
        return self._default(key)

    
