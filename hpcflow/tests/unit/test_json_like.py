# mypy: disable-error-code="annotation-unchecked"
from __future__ import annotations
from dataclasses import dataclass
import enum
from types import SimpleNamespace
from typing import Any

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.json_like import BaseJSONLike, ChildObjectSpec
from hpcflow.sdk.core.object_list import ObjectList

# BE AWARE THAT MYPY CANNOT CORRECTLY TYPE-CHECK THIS FILE AT ALL.
# It fails massively due to all the classes inside functions being passed to other functions.
# Omitting the types makes it ignore them all, which us for the best.


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


def test_json_like_name_is_name(null_config):
    spec = ChildObjectSpec(name="a")
    assert spec.json_like_name == "a"


@pytest.fixture
def obj_and_json_like_1(null_config):
    @dataclass
    class ObjA(BaseJSONLike):
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": 2.1,
    }
    return ObjA, js_1


def test_from_json_like_expected_obj_simple(obj_and_json_like_1):
    ObjA, js_1 = obj_and_json_like_1
    assert ObjA.from_json_like(js_1) == ObjA(**js_1)


def test_to_json_like_expected_json_like_simple(obj_and_json_like_1):
    ObjA, js_1 = obj_and_json_like_1
    js_2, _ = ObjA(**js_1).to_json_like()
    assert js_2 == js_1


def test_json_like_round_trip_obj_simple(obj_and_json_like_1):
    ObjA, js_1 = obj_and_json_like_1
    obj1 = ObjA(**js_1)
    js_2, _ = obj1.to_json_like()
    obj2 = ObjA.from_json_like(js_2)
    assert obj1 == obj2


@pytest.fixture
def BaseJSONLikeSubClass(null_config):
    return type("MyBaseJSONLike", (BaseJSONLike,), {})


def test_BaseJSONLike_child_object_class_namespace_via_obj(null_config):
    """Child object class passed directly as a class object."""

    @dataclass
    class ObjB(BaseJSONLike):
        c: int

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": {
            "c": 8,
        },
    }
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=ObjB(c=8))


def test_BaseJSONLike_child_object_class_namespace_via_name_and_dict_namespace(
    BaseJSONLikeSubClass: type[BaseJSONLike],
):
    """Child object class passed as a name and namespace passed as a dict."""
    T: type = BaseJSONLikeSubClass  # Workaround for python/mypy#14458

    @dataclass
    class ObjB(T):
        c: int

    @dataclass
    class ObjA(T):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_name="ObjB",
            ),
        )
        a: int
        b: float

    BaseJSONLikeSubClass._set_class_namespace({"ObjB": ObjB}, is_dict=True)

    js_1 = {
        "a": 1,
        "b": {
            "c": 8,
        },
    }
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=ObjB(c=8))


def test_BaseJSONLike_child_object_class_namespace_via_name_and_func_locals(
    BaseJSONLikeSubClass: type[BaseJSONLike],
):
    """Child object class passed as a name and namespace passed as function locals."""
    T: type = BaseJSONLikeSubClass  # Workaround for python/mypy#14458

    @dataclass
    class ObjB(T):
        c: int

    @dataclass
    class ObjA(T):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_name="ObjB",
            ),
        )
        a: int
        b: float

    BaseJSONLikeSubClass._set_class_namespace(locals(), is_dict=True)

    js_1 = {
        "a": 1,
        "b": {
            "c": 8,
        },
    }
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=ObjB(c=8))


def test_BaseJSONLike_child_object_class_namespace_via_name_and_SimpleNamespace(
    BaseJSONLikeSubClass: type[BaseJSONLike],
):
    """Child object class passed as a name and namespace passed as a SimpleNamespace."""
    T: type = BaseJSONLikeSubClass  # Workaround for python/mypy#14458

    @dataclass
    class ObjB(T):
        c: int

    @dataclass
    class ObjA(T):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_name="ObjB",
            ),
        )
        a: int
        b: float

    BaseJSONLikeSubClass._set_class_namespace(SimpleNamespace(ObjB=ObjB))

    js_1 = {
        "a": 1,
        "b": {
            "c": 8,
        },
    }
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=ObjB(c=8))


@pytest.fixture
def obj_and_child_obj_and_json_like_1(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        c: int

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": {
            "c": 8,
        },
    }

    return ObjA, ObjB, js_1


def test_from_json_like_expected_obj_with_child_obj(obj_and_child_obj_and_json_like_1):
    ObjA, ObjB, js_1 = obj_and_child_obj_and_json_like_1
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=ObjB(c=8))


def test_to_json_like_expected_json_like_with_child_obj(
    obj_and_child_obj_and_json_like_1,
):
    ObjA, ObjB, js_1 = obj_and_child_obj_and_json_like_1
    obj = ObjA(a=1, b=ObjB(c=js_1["b"]["c"]))
    js, _ = obj.to_json_like()
    assert js == js_1


def test_json_like_round_trip_with_child_obj(obj_and_child_obj_and_json_like_1):
    ObjA, ObjB, _ = obj_and_child_obj_and_json_like_1
    obj1 = ObjA(a=1, b=ObjB(c=4))
    js, _ = obj1.to_json_like()
    obj2 = ObjA.from_json_like(js)
    assert obj1 == obj2


@pytest.fixture
def obj_and_child_obj_with_json_like_name_and_json_like(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        c: int

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                json_like_name="json_b",
                class_obj=ObjB,
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "json_b": {
            "c": 8,
        },
    }

    return ObjA, ObjB, js_1


def test_from_json_like_expected_obj_with_child_obj_with_json_like_name(
    obj_and_child_obj_with_json_like_name_and_json_like,
):
    ObjA, ObjB, js_1 = obj_and_child_obj_with_json_like_name_and_json_like
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=ObjB(c=8))


def test_to_json_like_expected_json_like_with_child_obj_with_json_like_name(
    obj_and_child_obj_with_json_like_name_and_json_like,
):
    ObjA, ObjB, js_1 = obj_and_child_obj_with_json_like_name_and_json_like
    obj = ObjA(a=1, b=ObjB(c=js_1["json_b"]["c"]))
    js, _ = obj.to_json_like()
    assert js == js_1


def test_json_like_round_trip_with_child_obj_with_json_like_name(
    obj_and_child_obj_with_json_like_name_and_json_like,
):
    ObjA, ObjB, _ = obj_and_child_obj_with_json_like_name_and_json_like
    obj1 = ObjA(a=1, b=ObjB(c=4))
    obj2 = ObjA.from_json_like(obj1.to_json_like()[0])
    assert obj1 == obj2


@pytest.fixture
def obj_and_child_obj_with_list_and_json_like(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        c: int

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": [{"c": 8}, {"c": 9}],
    }

    return ObjA, ObjB, js_1


def test_from_json_like_expected_obj_with_child_obj_list(
    obj_and_child_obj_with_list_and_json_like,
):
    ObjA, ObjB, js_1 = obj_and_child_obj_with_list_and_json_like
    assert ObjA.from_json_like(js_1) == ObjA(a=1, b=[ObjB(c=8), ObjB(c=9)])


def test_to_json_like_expected_json_like_with_child_obj_list(
    obj_and_child_obj_with_list_and_json_like,
):
    ObjA, ObjB, js_1 = obj_and_child_obj_with_list_and_json_like
    obj = ObjA(a=1, b=[ObjB(c=js_1["b"][0]["c"]), ObjB(c=js_1["b"][1]["c"])])
    js, _ = obj.to_json_like()
    assert js == js_1


def test_json_like_round_trip_with_child_obj_list(
    obj_and_child_obj_with_list_and_json_like,
):
    ObjA, ObjB, _ = obj_and_child_obj_with_list_and_json_like
    obj1 = ObjA(a=1, b=[ObjB(c=4), ObjB(c=5)])
    obj2 = ObjA.from_json_like(obj1.to_json_like()[0])
    assert obj1 == obj2


@pytest.fixture
def obj_and_child_obj_with_dict_key_only_and_json_like_and_json_like_normed(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b", class_obj=ObjB, is_multiple=True, dict_key_attr="name"
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": {
            "c1": {"c": 8},
            "c2": {"c": 9},
        },
    }
    js_1_normed = {
        "a": 1,
        "b": [
            {"name": "c1", "c": 8},
            {"name": "c2", "c": 9},
        ],
    }
    return ObjA, ObjB, js_1, js_1_normed


def test_from_json_like_expected_obj_with_child_obj_dict_key_only(
    obj_and_child_obj_with_dict_key_only_and_json_like_and_json_like_normed,
):
    (
        ObjA,
        ObjB,
        js_1,
        _,
    ) = obj_and_child_obj_with_dict_key_only_and_json_like_and_json_like_normed
    assert ObjA.from_json_like(js_1) == ObjA(
        a=1,
        b=[ObjB(name="c1", c=8), ObjB(name="c2", c=9)],
    )


def test_to_json_like_expected_json_like_with_child_obj_dict_key_only(
    obj_and_child_obj_with_dict_key_only_and_json_like_and_json_like_normed,
):
    (
        ObjA,
        ObjB,
        js_1,
        js_1_normed,
    ) = obj_and_child_obj_with_dict_key_only_and_json_like_and_json_like_normed
    obj = ObjA(
        a=1,
        b=[
            ObjB(name="c1", c=js_1["b"]["c1"]["c"]),
            ObjB(name="c2", c=js_1["b"]["c2"]["c"]),
        ],
    )
    js, _ = obj.to_json_like()
    assert js == js_1_normed


@pytest.fixture
def obj_and_child_obj_with_dict_key_val_and_json_like_and_json_like_normed(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                dict_key_attr="name",
                dict_val_attr="c",
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": {
            "c1": 8,
            "c2": 9,
        },
    }
    js_1_normed = {
        "a": 1,
        "b": [
            {"name": "c1", "c": 8},
            {"name": "c2", "c": 9},
        ],
    }
    return ObjA, ObjB, js_1, js_1_normed


def test_from_json_like_expected_obj_with_child_obj_dict_key_dict_val(
    obj_and_child_obj_with_dict_key_val_and_json_like_and_json_like_normed,
):
    (
        ObjA,
        ObjB,
        js_1,
        _,
    ) = obj_and_child_obj_with_dict_key_val_and_json_like_and_json_like_normed
    assert ObjA.from_json_like(js_1) == ObjA(
        a=1,
        b=[ObjB(name="c1", c=8), ObjB(name="c2", c=9)],
    )


def test_to_json_like_expected_json_like_with_child_obj_dict_key_dict_val(
    obj_and_child_obj_with_dict_key_val_and_json_like_and_json_like_normed,
):
    (
        ObjA,
        ObjB,
        js_1,
        js_1_normed,
    ) = obj_and_child_obj_with_dict_key_val_and_json_like_and_json_like_normed
    obj = ObjA(
        a=1,
        b=[
            ObjB(name="c1", c=js_1["b"]["c1"]),
            ObjB(name="c2", c=js_1["b"]["c2"]),
        ],
    )
    js, _ = obj.to_json_like()
    assert js == js_1_normed


def test_from_json_like_raise_on_is_multiple_with_dict_but_no_dict_key_attr(null_config):
    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": {
            "c1": 8,
            "c2": 9,
        },
    }

    with pytest.raises(ValueError):
        ObjA.from_json_like(js_1)


def test_from_json_like_raise_on_is_multiple_with_dict_key_no_dict_val_but_non_dict_vals(
    null_config,
):
    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
                dict_key_attr="name",
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": {
            "c1": 8,
            "c2": 9,
        },
    }
    with pytest.raises(TypeError):
        ObjA.from_json_like(js_1)


def test_from_json_like_raise_on_is_multiple_not_list_or_dict(null_config):
    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
            ),
        )
        a: int
        b: float

    js_1 = {
        "a": 1,
        "b": 2,
    }
    with pytest.raises(TypeError):
        ObjA.from_json_like(js_1)


def test_from_json_like_with_parent_ref(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        name: str
        c: int
        obj_A: Any = None

        def __eq__(self, other):
            if not isinstance(other, self.__class__):
                return False
            return self.name == other.name and self.c == other.c

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                parent_ref="obj_A",
            ),
        )
        a: int
        b: float

        def __post_init__(self):
            self._set_parent_refs()

        def __eq__(self, other):
            if not isinstance(other, self.__class__):
                return False
            return (
                self.a == other.a
                and self.b == other.b
                and self.b.obj_A is self
                and other.b.obj_A is other
            )

    js_1 = {
        "a": 1,
        "b": {
            "name": "c1",
            "c": 8,
        },
    }

    objA = ObjA(a=1, b=ObjB(name=js_1["b"]["name"], c=js_1["b"]["c"]))
    objA.b.obj_A = objA

    assert ObjA.from_json_like(js_1) == objA


def test_json_like_round_trip_with_parent_ref(null_config):
    @dataclass
    class ObjB(BaseJSONLike):
        name: str
        c: int
        obj_A: Any = None

        def __eq__(self, other):
            if not isinstance(other, self.__class__):
                return False
            return self.name == other.name and self.c == other.c

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                parent_ref="obj_A",
            ),
        )
        a: int
        b: float

        def __eq__(self, other):
            if not isinstance(other, self.__class__):
                return False
            return (
                self.a == other.a
                and self.b == other.b
                and self.b.obj_A is self
                and other.b.obj_A is other
            )

    js_1 = {
        "a": 1,
        "b": {
            "name": "c1",
            "c": 8,
        },
    }

    objA = ObjA.from_json_like(js_1)
    assert objA.to_json_like()[0] == js_1


def test_from_json_like_optional_attr(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
            ),
        )
        a: int
        b: Any = None

    js_in = {"a": 9, "b": None}
    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(a=9)


def test_from_json_like_optional_attr_with_is_multiple_both_none(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
                class_obj=ObjB,
            ),
        )
        a: int
        b: Any = None

    js_in = {
        "a": 9,
        "b": [None, None],
    }
    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(a=9, b=[None, None])


def test_from_json_like_optional_attr_with_is_multiple_one_none(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
                class_obj=ObjB,
            ),
        )
        a: int
        b: Any = None

    js_in = {
        "a": 9,
        "b": [None, {"name": "c1", "c": 2}],
    }
    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(a=9, b=[None, ObjB(name="c1", c=2)])


def test_from_json_like_optional_attr_with_is_multiple_one_none_and_shared_data_name(
    null_config,
):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
                class_obj=ObjB,
                shared_data_name="bees",
                shared_data_primary_key="name",
            ),
        )
        a: int
        b: Any = None

    dcts = [
        {"name": "c1", "c": 2},
        {"name": "c2", "c": 3},
    ]

    obj_lst = ObjectList([ObjB(**i) for i in dcts])

    js_in = {
        "a": 9,
        "b": [None, "c1"],
    }
    objA = ObjA.from_json_like(js_in, shared_data={"bees": obj_lst})
    assert objA == ObjA(a=9, b=[None, ObjB(name="c1", c=2)])


def test_from_json_like_optional_attr_with_is_multiple_all_none_and_shared_data_name(
    null_config,
):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                is_multiple=True,
                class_obj=ObjB,
                shared_data_name="bees",
                shared_data_primary_key="name",
            ),
        )
        a: int
        b: Any = None

    dcts = [
        {"name": "c1", "c": 2},
        {"name": "c2", "c": 3},
    ]

    obj_lst = ObjectList([ObjB(**i) for i in dcts])

    js_in = {
        "a": 9,
        "b": [None, None],
    }
    objA = ObjA.from_json_like(js_in, shared_data={"bees": obj_lst})
    assert objA == ObjA(a=9, b=[None, None])


def test_from_json_like_optional_attr_with_shared_data_name(null_config):
    # test optional attribute with shared_data_name
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                shared_data_name="bees",
                shared_data_primary_key="name",
            ),
        )
        a: int
        b: Any = None

    dcts = [
        {"name": "c1", "c": 2},
        {"name": "c2", "c": 3},
    ]

    obj_lst = ObjectList([ObjB(**i) for i in dcts])

    js_in = {
        "a": 9,
        "b": None,
    }
    objA = ObjA.from_json_like(js_in, shared_data={"bees": obj_lst})
    assert objA == ObjA(a=9, b=None)


def test_from_json_like_optional_attr_with_enum(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    class MyEnum(enum.Enum):
        A = 0
        B = 1
        C = 2

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (ChildObjectSpec(name="b", class_obj=MyEnum, is_enum=True),)
        a: int
        b: Any = None

    js_in = {
        "a": 9,
        "b": None,
    }
    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(a=9, b=None)


def test_from_json_like_with_is_multiple(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (ChildObjectSpec(name="b", class_obj=ObjB, is_multiple=True),)
        a: int
        b: Any

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": [{"name": "c1", "c": 2}, {"name": "c2", "c": 3}],  # multiple
    }

    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(a=9, b=[ObjB(name="c1", c=2), ObjB(name="c2", c=3)])


def test_from_json_like_with_is_multiple_dict_values(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                is_dict_values=True,
            ),
        )
        a: int
        b: Any

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": {
            "key1": {"name": "c1", "c": 2},
            "key2": {"name": "c2", "c": 3},
        },  # multiple dict values, arbitrary keys, dict structure will be maintained
    }

    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(
        a=9,
        b={"key1": ObjB(name="c1", c=2), "key2": ObjB(name="c2", c=3)},
    )


def test_from_json_like_with_is_multiple_dict_values_ensure_list(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                is_dict_values=True,
                is_dict_values_ensure_list=True,
            ),
        )
        a: int
        b: Any

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": {
            "key1": {"name": "c1", "c": 2},
            "key2": [{"name": "c2", "c": 3}, {"name": "c3", "c": 4}],
        },
        # multiple dict values (and multiple items for each), arbitrary keys, dict
        # structure will be maintained
    }

    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(
        a=9,
        b={
            "key1": [ObjB(name="c1", c=2)],
            "key2": [ObjB(name="c2", c=3), ObjB(name="c3", c=4)],
        },
    )


def test_from_json_like_round_trip_with_is_multiple_dict_values(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                is_dict_values=True,
            ),
        )
        a: int
        b: Any

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": {
            "key1": {"name": "c1", "c": 2},
            "key2": {"name": "c2", "c": 3},
        },  # multiple dict values, arbitrary keys, dict structure will be maintained
    }

    objA = ObjA.from_json_like(js_in)
    assert objA.to_json_like()[0] == js_in


def test_from_json_like_round_trip_with_is_multiple_dict_values_ensure_list(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                is_dict_values=True,
                is_dict_values_ensure_list=True,
            ),
        )
        a: int
        b: Any

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": {
            "key1": [{"name": "c1", "c": 2}],
            "key2": [{"name": "c2", "c": 3}, {"name": "c3", "c": 4}],
        },
        # multiple dict values (and multiple items for each), arbitrary keys, dict
        # structure will be maintained
    }

    objA = ObjA.from_json_like(js_in)
    assert objA.to_json_like()[0] == js_in


def test_from_json_like_with_is_multiple_and_shared_data(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                shared_data_name="bees",
                shared_data_primary_key="name",
            ),
        )
        a: int
        b: Any

    dcts = [
        {"name": "c1", "c": 2},
        {"name": "c2", "c": 3},
    ]

    obj_lst = ObjectList([ObjB(**i) for i in dcts])

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": ["c1", "c2"],  # multiple from shared
    }

    objA = ObjA.from_json_like(js_in, shared_data={"bees": obj_lst})
    assert objA == ObjA(a=9, b=[ObjB(name="c1", c=2), ObjB(name="c2", c=3)])


def test_from_json_like_with_is_multiple_and_shared_data_dict_lookup(null_config):
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    @dataclass
    class ObjB(BaseJSONLikeSubClass):
        name: str
        c: int
        _hash_value: str | None = None

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                is_multiple=True,
                dict_key_attr="name",
                shared_data_name="bees",
                shared_data_primary_key="name",
            ),
        )
        a: int
        b: Any

    dcts = [
        {"name": "c1", "c": 2},
        {"name": "c2", "c": 3},
    ]

    obj_lst = ObjectList([ObjB(**i) for i in dcts])

    # e.g. from data files:
    js_in = {
        "a": 9,
        "b": {
            "c1": {"c": 2},
            "c2": {"c": 3},
        },  # multiple from shared as dict with lookup kwargs
    }

    objA = ObjA.from_json_like(js_in, shared_data={"bees": obj_lst})
    assert objA == ObjA(a=9, b=[ObjB(name="c1", c=2), ObjB(name="c2", c=3)])


def test_from_json_like_enum(null_config):
    # test enum from_json_like
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    class MyEnum(enum.Enum):
        A = 0
        B = 1
        C = 2

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (ChildObjectSpec(name="b", class_obj=MyEnum, is_enum=True),)
        a: int
        b: Any

    js_in = {
        "a": 9,
        "b": "A",
    }
    objA = ObjA.from_json_like(js_in)
    assert objA == ObjA(a=9, b=MyEnum.A)


def test_from_to_json_round_trip_enum(null_config):
    # test enum round trip
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    class MyEnum(enum.Enum):
        A = 0
        B = 1
        C = 2

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (ChildObjectSpec(name="b", class_obj=MyEnum, is_enum=True),)
        a: int
        b: Any

    js_in = {
        "a": 9,
        "b": "A",
    }
    objA = ObjA.from_json_like(js_in)
    assert objA.to_json_like()[0] == js_in


def test_from_json_like_round_trip_enum_case_insensitivity(null_config):
    # test enum from_json_like case-insensitivity
    BaseJSONLikeSubClass = type("MyBaseJSONLike", (BaseJSONLike,), {})

    class MyEnum(enum.Enum):
        A = 0
        B = 1
        C = 2

    @dataclass
    class ObjA(BaseJSONLikeSubClass):
        _child_objects = (ChildObjectSpec(name="b", class_obj=MyEnum, is_enum=True),)
        a: int
        b: Any

    js_in_1 = {
        "a": 9,
        "b": "a",
    }
    js_in_2 = {
        "a": 9,
        "b": "A",
    }
    objA_1 = ObjA.from_json_like(js_in_1)
    objA_2 = ObjA.from_json_like(js_in_2)
    assert objA_1 == objA_2


@pytest.mark.skip(
    reason=(
        "We currently cull parent references in `JSONLike.to_dict`. This should ideally "
        "be in BaseJSONLike.to_dict, which would allow this test to pass. However, "
        "culling involves looping over app._core_classes, which we cannot access from "
        "this class."
    )
)
def test_to_json_like_with_child_ref(null_config):
    """i.e. check parent references are not included in child item to_json_like output:"""

    @dataclass
    class ObjB(BaseJSONLike):
        name: str
        c: int
        obj_A: Any = None

    @dataclass
    class ObjA(BaseJSONLike):
        _child_objects = (
            ChildObjectSpec(
                name="b",
                class_obj=ObjB,
                parent_ref="obj_A",
            ),
        )
        a: int
        b: float

        def __post_init__(self):
            self._set_parent_refs()

    objB = ObjB(name="c1", c=8)
    objA = ObjA(a=1, b=objB)

    js_1 = {
        "a": 1,
        "b": {
            "name": "c1",
            "c": 8,
        },
    }

    objA = ObjA.from_json_like(js_1)

    assert objA.b.obj_A == objA and objA.b.to_json_like()[0] == js_1["b"]
