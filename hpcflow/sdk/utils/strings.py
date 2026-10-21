from typing import Iterable
import ast
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


def add_import_timing(py_str: str) -> str:
    """Add timing instrumentation to user Python scripts."""
    tree = ast.parse(py_str)
    body = tree.body

    start_line = 0
    idx = 0

    # Module docstring
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        doc_stmt = body[0]
        assert doc_stmt.end_lineno is not None
        start_line = doc_stmt.end_lineno
        idx = 1

    # __future__ imports
    while idx < len(body):
        future_stmt = body[idx]
        if not isinstance(future_stmt, ast.ImportFrom):
            break
        if future_stmt.module != "__future__":
            break

        assert future_stmt.end_lineno is not None
        start_line = future_stmt.end_lineno
        idx += 1

    # User imports
    import_end_line = start_line
    while idx < len(body):
        import_stmt = body[idx]
        if not isinstance(import_stmt, (ast.Import, ast.ImportFrom)):
            break

        assert import_stmt.end_lineno is not None
        import_end_line = import_stmt.end_lineno
        idx += 1

    lines = py_str.splitlines(keepends=True)

    # Insert bottom one first so line numbers remain valid
    lines.insert(
        import_end_line,
        "\n_user_import_time = time.perf_counter() - _script_start\n",
    )

    lines.insert(
        start_line,
        "\nimport time\n_script_start = time.perf_counter()\n",
    )

    return "".join(lines)


def capitalise_first_letter(chars: str) -> str:
    """
    Convert the first character of a string to upper case (if that makes sense).
    The rest of the string is unchanged.
    """
    return chars[0].upper() + chars[1:]


SECRET_PATTERNS = [
    r"(?i)api[_-]?key\s*=\s*['\"]?[A-Za-z0-9_\-]{16,}",
    r"(?i)username\s*=\s*['\"]?.+",
    r"(?i)password\s*=\s*['\"]?.+",
    r"(?i)secret\s*=\s*['\"]?.+",
    r"AKIA[0-9A-Z]{16}",  # AWS access key
]


def looks_like_secret(text: str) -> bool:
    """Return True if a string looks like it might contain a secret credential."""
    for pattern in SECRET_PATTERNS:
        if re.search(pattern, text):
            return True
    return False
