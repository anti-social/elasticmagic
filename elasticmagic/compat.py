import sys
PY2 = sys.version_info[0] == 2


if not PY2:
    text_type = str
    string_types = (str,)
    binary_type = str
    unichr = chr
    int_types = (int,)
else:
    text_type = unicode
    string_types = (str, unicode)
    binary_type = bytes
    unichr = unichr
    int_types = (int, long)


def with_metaclass(meta, *bases):
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__
        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('temporary_class', None, {})
