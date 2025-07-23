from typing import Iterable
import re


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


def extract_py_from_future_imports(py_str: str) -> tuple[str, set[str]]:
    """
    Remove any `from __future__ import <feature>` lines from a string of Python code, and
    return the modified string, and a list of `<feature>`s that were imported.

    Notes
    -----
    This is required when generated a combined-scripts jobscript that concatenates
    multiple Python scripts into one script. If `__future__` statements are included in
    these individual scripts, they must be moved to the top of the file [1].

    References
    ----------
    [1] https://docs.python.org/3/reference/simple_stmts.html#future-statements

    """

    pattern = r"^from __future__ import (.*)\n"
    if future_imports := (set(re.findall(pattern, py_str, flags=re.MULTILINE) or ())):
        future_imports = {
            j.strip() for i in future_imports for j in i.split(",") if j.strip()
        }
        py_str = re.sub(pattern, "", py_str, flags=re.MULTILINE)

    return (py_str, future_imports)


def capitalise_first_letter(chars: str) -> str:
    """
    Convert the first character of a string to upper case (if that makes sense).
    The rest of the string is unchanged.
    """
    return chars[0].upper() + chars[1:]
