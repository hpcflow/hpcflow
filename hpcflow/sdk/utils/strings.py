from typing import Iterable


def shorten_list_str(
    lst: Iterable, items: int = 10, end_num: int = 1, placeholder: str = "..."
) -> str:
    """Format a list as a string, including only some maximum number of items.

    Parameters
    ----------
    lst:
        The list to format in a shortened form.
    items:
        The total number of items to include in the formatted list.
    end_num:
        The number of items to include at the end of the formatted list.
    placeholder
        The placeholder to use to replace excess items in the formatted list.

    Examples
    --------
    >>> shorten_list_str(list(range(20)), items=5)
    '[0, 1, 2, 3, ..., 19]'

    """
    lst = list(lst)
    if len(lst) <= items + 1:  # (don't replace only one item)
        lst_short = lst
    else:
        start_num = items - end_num
        lst_short = lst[:start_num] + ["..."] + lst[-end_num:]

    return "[" + ", ".join(f"{i}" for i in lst_short) + "]"
