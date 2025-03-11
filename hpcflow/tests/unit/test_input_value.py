from __future__ import annotations
import sys
from pathlib import Path
from typing import TYPE_CHECKING
import pytest
import requests

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import P1_parameter_cls as P1

if TYPE_CHECKING:
    from hpcflow.sdk.core.parameters import Parameter


@pytest.fixture
def null_config(tmp_path: Path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.fixture
def param_p1(null_config) -> Parameter:
    return hf.Parameter("p1")


def test_fix_trailing_path_delimiter(param_p1: Parameter):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="a.")
    iv2 = hf.InputValue(parameter=param_p1, value=101, path="a")
    assert iv1.path == iv2.path


def test_fix_single_path_delimiter(param_p1: Parameter):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path=".")
    iv2 = hf.InputValue(parameter=param_p1, value=101)
    assert iv1.path == iv2.path


def test_normalised_path_without_path(param_p1: Parameter):
    iv1 = hf.InputValue(parameter=param_p1, value=101)
    assert iv1.normalised_path == "inputs.p1"


def test_normalised_path_with_single_element_path(param_p1: Parameter):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="a")
    assert iv1.normalised_path == "inputs.p1.a"


def test_normalised_path_with_multi_element_path(param_p1: Parameter):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="a.b")
    assert iv1.normalised_path == "inputs.p1.a.b"


def test_normalised_path_with_empty_path(param_p1: Parameter):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="")
    assert iv1.normalised_path == "inputs.p1"


def test_resource_spec_get_param_path(null_config) -> None:
    rs1 = hf.ResourceSpec()
    assert rs1.normalised_path == "resources.any"


def test_resource_spec_get_param_path_scope_any_with_single_kwarg(null_config) -> None:
    rs1 = hf.ResourceSpec(scratch="local")
    assert rs1.normalised_path == "resources.any"


def test_resources_spec_get_param_path_scope_main(null_config) -> None:
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.main())
    assert rs1.normalised_path == "resources.main"


def test_resources_spec_get_param_path_scope_with_kwargs(null_config) -> None:
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.input_file_generator(file="file1"))
    assert rs1.normalised_path == "resources.input_file_generator[file=file1]"


def test_resources_spec_get_param_path_scope_with_no_kwargs(null_config) -> None:
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.input_file_generator())
    assert rs1.normalised_path == "resources.input_file_generator"


def test_input_value_from_json_like_class_method_attribute_is_set(null_config) -> None:
    parameter_typ = "p1"
    cls_method = "from_data"
    json_like = {"parameter": f"{parameter_typ}::{cls_method}", "value": 101}
    inp_val = hf.InputValue.from_json_like(json_like, shared_data=hf.template_components)
    assert inp_val.parameter.typ == parameter_typ
    assert inp_val.value_class_method == cls_method


def test_value_sequence_from_json_like_class_method_attribute_is_set(null_config) -> None:
    parameter_typ = "p1"
    cls_method = "from_data"
    json_like = {
        "path": f"inputs.{parameter_typ}::{cls_method}",
        "values": [101],
        "nesting_order": 0,
    }

    val_seq = hf.ValueSequence.from_json_like(
        json_like, shared_data=hf.template_components
    )
    assert val_seq.value_class_method == cls_method


def test_path_attributes(null_config) -> None:
    inp = hf.InputValue(parameter="p1", value=101, path="a.b")
    assert inp.labelled_type == "p1"
    assert inp.normalised_path == "inputs.p1.a.b"
    assert inp.normalised_inputs_path == "p1.a.b"


def test_path_attributes_with_label_arg(null_config) -> None:
    inp = hf.InputValue(parameter="p1", value=101, path="a.b", label="1")
    assert inp.labelled_type == "p1[1]"
    assert inp.normalised_path == "inputs.p1[1].a.b"
    assert inp.normalised_inputs_path == "p1[1].a.b"


def test_path_attributes_with_label_arg_cast(null_config) -> None:
    inp = hf.InputValue(parameter="p1", value=101, path="a.b", label=1)
    assert inp.labelled_type == "p1[1]"
    assert inp.normalised_path == "inputs.p1[1].a.b"
    assert inp.normalised_inputs_path == "p1[1].a.b"


def test_from_json_like(null_config) -> None:
    inp = hf.InputValue.from_json_like(
        json_like={"parameter": "p1", "value": 101},
        shared_data=hf.template_components,
    )
    assert inp.parameter.typ == hf.Parameter("p1").typ
    assert inp.value == 101
    assert inp.label == ""


def test_from_json_like_with_label(null_config) -> None:
    inp = hf.InputValue.from_json_like(
        json_like={"parameter": "p1[1]", "value": 101},
        shared_data=hf.template_components,
    )
    assert inp.parameter.typ == hf.Parameter("p1").typ
    assert inp.value == 101
    assert inp.label == "1"


def test_value_is_dict_check_success(null_config) -> None:
    # Parameter("p1c") has an associated `ParameterValue` class, so data should be a dict:
    hf.InputValue("p1c", {"a": 101})


def test_value_is_dict_check_raise(null_config) -> None:
    # Parameter("p1c") has an associated `ParameterValue` class so data should be a dict:
    with pytest.raises(ValueError):
        hf.InputValue("p1c", 101)


def test_value_is_dict_check_no_raise_if_sub_parameter(null_config):
    # Parameter("p1c") has an associated `ParameterValue` class, but the specified value
    # is for some sub-data:
    hf.InputValue("p1c", path="a", value=101)


@pytest.mark.xfail(
    condition=sys.platform == "darwin",
    raises=requests.exceptions.HTTPError,
    reason=(
        "GHA MacOS runners use the same IP address, so we get rate limited when "
        "retrieving demo data from GitHub."
    ),
)
def test_demo_data_value(null_config) -> None:
    name = "text_file.txt"
    assert hf.InputValue("p1", value=f"<<demo_data_file:{name}>>").value == str(
        hf.demo_data_cache_dir.joinpath(name)
    )
