from __future__ import annotations
from dataclasses import dataclass
from typing_extensions import TypedDict
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.object_list import ObjectList, DotAccessObjectList


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@dataclass
class MyObj:
    name: str
    data: int


class SimpleObjectList(TypedDict):
    objects: list[MyObj]
    object_list: DotAccessObjectList


@pytest.fixture
def simple_object_list(null_config) -> SimpleObjectList:
    my_objs = [MyObj(name="A", data=1), MyObj(name="B", data=2)]
    obj_list = DotAccessObjectList(my_objs, access_attribute="name")
    return {"objects": my_objs, "object_list": obj_list}


def test_get_item(simple_object_list: SimpleObjectList):
    objects = simple_object_list["objects"]
    obj_list = simple_object_list["object_list"]

    assert obj_list[0] == objects[0] and obj_list[1] == objects[1]


def test_get_dot_notation(simple_object_list: SimpleObjectList):
    objects = simple_object_list["objects"]
    obj_list = simple_object_list["object_list"]

    assert obj_list.A == objects[0] and obj_list.B == objects[1]


def test_add_obj_to_end(simple_object_list: SimpleObjectList):
    obj_list = simple_object_list["object_list"]
    new_obj = MyObj("C", 3)
    obj_list.add_object(new_obj)
    assert obj_list[-1] == new_obj


def test_add_obj_to_start(simple_object_list: SimpleObjectList):
    obj_list = simple_object_list["object_list"]
    new_obj = MyObj("C", 3)
    obj_list.add_object(new_obj, 0)
    assert obj_list[0] == new_obj


def test_add_obj_to_middle(simple_object_list: SimpleObjectList):
    obj_list = simple_object_list["object_list"]
    new_obj = MyObj("C", 3)
    obj_list.add_object(new_obj, 1)
    assert obj_list[1] == new_obj


def test_get_obj_attr_custom_callable(null_config):
    def my_get_obj_attr(self, obj, attr):
        if attr == "a":
            return getattr(obj, attr)
        else:
            return getattr(obj, "b")[attr]

    MyObjectList = type("MyObjectList", (ObjectList,), {})
    MyObjectList._get_obj_attr = my_get_obj_attr

    o1 = MyObjectList(
        [
            {"a": 1, "b": {"c1": 2}},
            {"a": 2, "b": {"c1": 3}},
        ]
    )
    assert o1.get(c1=2) == o1[0]


def test_get_with_missing_key(null_config) -> None:
    o1 = ObjectList([{"a": 1}, {"b": 2}])
    assert o1.get(a=1) == {"a": 1}


def test_parameters_list_get_equivalence(null_config) -> None:
    p_name = "p12334567898765432101"
    hf.parameters.add_object(hf.Parameter(p_name))
    assert p_name in hf.parameters.list_attrs()
    assert (
        getattr(hf.parameters, p_name)
        == hf.parameters.get(p_name)
        == hf.parameters.get_all(p_name)[0]
        == hf.parameters.get(typ=p_name)
    )


def test_parameters_list_get_equivalence_non_existent(new_null_config) -> None:
    # non-existent parameters should be created, unlike other ObjectList sub-classes,
    # which raise
    hf.reload_template_components()
    p_name = "p12334567898765432101"
    assert p_name not in hf.parameters.list_attrs()
    assert (
        getattr(hf.parameters, p_name)
        == hf.parameters.get(p_name)
        == hf.parameters.get_all(p_name)[0]
        == hf.parameters.get(typ=p_name)
    )
