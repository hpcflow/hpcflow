from hpcflow.sdk.core.cache import ObjectCache
from hpcflow.sdk.core.test_utils import make_workflow


def test_object_cache_recursive_element_dependents(tmp_path):
    wk = make_workflow(
        schemas_spec=(
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, ("p3",), "t2"),
            ({"p3": None}, ("p4",), "t3"),
            ({"p4": None}, ("p5",), "t4"),
        ),
        path=tmp_path,
        local_inputs={0: ("p1",)},
        overwrite=True,
    )
    obj_cache = ObjectCache.build(wk, dependencies=True)
    assert obj_cache.elem_elem_dependents_rec == {
        0: {1, 2, 3},
        1: {2, 3},
        2: {3},
        3: set(),
    }
