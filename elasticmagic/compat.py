def force_unicode(value):
    """
    Forces bytes to become a string.
    """
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='replace')
    elif not isinstance(value, str):
        value = str(value)

    return value
