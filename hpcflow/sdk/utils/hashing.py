def get_hash(obj):
    """Return a hash from an arbitrarily nested structure dicts, lists, tuples, and
    sets.

    Note the resulting hash is not necessarily stable across sessions or machines.
    """

    if isinstance(obj, set):
        return hash((set, frozenset(get_hash(i) for i in obj)))

    if isinstance(obj, (tuple, list)):
        return hash((type(obj), *(get_hash(i) for i in obj)))

    if isinstance(obj, dict):
        return hash(frozenset((k, get_hash(v)) for k, v in obj.items()))

    return hash(obj)
