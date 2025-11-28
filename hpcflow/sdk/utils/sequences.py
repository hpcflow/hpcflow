def _add_list_items(lst, target, item_factory):
    """Extend a list to a target size, filling in new items using a factory
    callable."""
    len_diff = target - len(lst)
    if len_diff > 0:
        lst += [item_factory() for _ in range(len_diff)]
