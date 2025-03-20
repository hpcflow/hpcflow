from hpcflow.sdk.utils.hashing import get_hash


def test_get_hash_simple_types_is_int():
    assert isinstance(get_hash(1), int)
    assert isinstance(get_hash(3.2), int)
    assert isinstance(get_hash("a"), int)
    assert isinstance(get_hash("abc"), int)


def test_get_hash_compound_types_is_int():
    assert isinstance(get_hash([1, 2, 3]), int)
    assert isinstance(get_hash((1, 2, 3)), int)
    assert isinstance(get_hash({1, 2, 3}), int)
    assert isinstance(get_hash({"a": 1, "b": 2, "c": 3}), int)


def test_get_hash_nested_dict_is_int():
    assert isinstance(get_hash({"a": {"b": {"c": [1, 2, 3, ("4", 5, 6)]}}}), int)


def test_get_hash_distinct_simple_types():
    assert get_hash(1) != get_hash(2)
    assert get_hash(2.2) != get_hash(2.3)
    assert get_hash("a") != get_hash("b")
    assert get_hash("abc") != get_hash("ABC")


def test_get_hash_distinct_compound_types():
    assert get_hash([1, 2, 3]) != get_hash([1, 2, 4])
    assert get_hash((1, 2, 3)) != get_hash((1, 2, 4))
    assert get_hash({1, 2, 3}) != get_hash({1, 2, 4})
    assert get_hash({"a": 1, "b": 2, "c": 3}) != get_hash({"a": 1, "b": 2, "c": 4})
    assert get_hash({"a": {"b": {"c": [1, 2, 3, ("4", 5, 7)]}}}) == get_hash(
        {"a": {"b": {"c": [1, 2, 3, ("4", 5, 7)]}}}
    )
    assert get_hash({"a": 1}) != get_hash(1) != get_hash("a")


def test_get_hash_equal_simple_types():
    assert get_hash(1) == get_hash(1)
    assert get_hash(2.2) == get_hash(2.2)
    assert get_hash("a") == get_hash("a")
    assert get_hash("abc") == get_hash("abc")


def test_get_hash_equal_compound_types():
    assert get_hash([1, 2, 3]) == get_hash([1, 2, 3])
    assert get_hash((1, 2, 3)) == get_hash((1, 2, 3))
    assert get_hash({1, 2, 3}) == get_hash({1, 2, 3})
    assert get_hash({"a": 1, "b": 2, "c": 3}) == get_hash({"a": 1, "b": 2, "c": 3})
    assert get_hash({"a": {"b": {"c": [1, 2, 3, ("4", 5, 6)]}}}) == get_hash(
        {"a": {"b": {"c": [1, 2, 3, ("4", 5, 6)]}}}
    )


def test_get_hash_order_insensitivity():
    assert get_hash({"a": 1, "b": 2}) == get_hash({"b": 2, "a": 1})
    assert get_hash({1, 2, 3}) == get_hash({2, 3, 1})


def test_get_hash_order_sensitivity():
    assert get_hash([1, 2, 3]) != get_hash([2, 3, 1])
    assert get_hash((1, 2, 3)) != get_hash((2, 3, 1))
    assert get_hash("abc") != get_hash("cba")
