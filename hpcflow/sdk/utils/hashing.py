def get_hash(obj):
    """Return a hash from an arbitrarily nested structure dicts, lists, tuples, and
    sets.

    Note the resulting hash is not necessarily stable across sessions or machines.
    """

    if isinstance(obj, (set, tuple, list)):
        return hash(tuple([type(obj)] + [get_hash(i) for i in obj]))

    elif not isinstance(obj, dict):
        return hash(obj)

    new_obj = {k: get_hash(obj[k]) for k in obj}

    return hash(frozenset(sorted(new_obj.items())))
