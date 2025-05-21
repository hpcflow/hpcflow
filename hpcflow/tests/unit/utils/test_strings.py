from textwrap import dedent

from hpcflow.sdk.utils.strings import extract_py_from_future_imports


def test_extract_py_from_future_imports_none():
    py_str = dedent(
        """\

    def my_function():
        print("blah!")
    """
    )
    new_str, imports = extract_py_from_future_imports(py_str)
    assert imports == set()
    assert new_str == py_str


def test_extract_py_from_future_imports_single():
    py_str = dedent(
        """\
    from __future__ import annotations

    def my_function():
        print("blah!")
    """
    )
    new_str, imports = extract_py_from_future_imports(py_str)
    assert imports == {"annotations"}
    assert new_str == dedent(
        """\

    def my_function():
        print("blah!")
    """
    )


def test_extract_py_from_future_imports_multi():
    py_str = dedent(
        """\
    from __future__ import annotations, feature_2

    def my_function():
        print("blah!")
    """
    )
    new_str, imports = extract_py_from_future_imports(py_str)
    assert imports == {"annotations", "feature_2"}
    assert new_str == dedent(
        """\

    def my_function():
        print("blah!")
    """
    )


def test_extract_py_from_future_imports_trailing_comma():
    py_str = dedent(
        """\
    from __future__ import annotations,

    def my_function():
        print("blah!")
    """
    )
    new_str, imports = extract_py_from_future_imports(py_str)
    assert imports == {"annotations"}
    assert new_str == dedent(
        """\

    def my_function():
        print("blah!")
    """
    )


def test_extract_py_from_future_imports_multi_lines():
    py_str = dedent(
        """\
    from __future__ import annotations, feature_2
    from __future__ import feature_2, feature_3,

    def my_function():
        print("blah!")
    """
    )
    new_str, imports = extract_py_from_future_imports(py_str)
    assert imports == {"annotations", "feature_2", "feature_3"}
    assert new_str == dedent(
        """\

    def my_function():
        print("blah!")
    """
    )
