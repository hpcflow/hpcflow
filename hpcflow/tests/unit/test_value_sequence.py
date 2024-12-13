import sys
import numpy as np
from pathlib import Path
import pytest
import requests

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import P1_parameter_cls as P1


@pytest.fixture
def null_config(tmp_path: Path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


def test_inputs_value_sequence_label_added_to_path():
    seq = hf.ValueSequence(path="inputs.p1.a", values=[0, 1], nesting_order=0, label=0)
    assert seq.path == "inputs.p1[0].a"


def test_inputs_value_sequence_no_label_added_to_path():
    seq = hf.ValueSequence(path="inputs.p1.a", values=[0, 1], nesting_order=0, label="")
    assert seq.path == "inputs.p1.a"


def test_inputs_value_sequence_label_attr_added():
    seq = hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0)
    assert seq.label == "1"


def test_inputs_value_sequence_label_path_unmodified():
    path = "inputs.p1[1].a"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path


def test_raise_on_inputs_value_sequence_label_path_unequal():
    with pytest.raises(ValueError):
        hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0, label="2")


def test_no_raise_on_inputs_value_sequence_label_path_equal():
    hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0, label="1")


def test_no_raise_on_inputs_value_sequence_label_path_cast_equal():
    hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0, label=1)


def test_raise_on_resources_value_sequence_with_path_label():
    with pytest.raises(ValueError):
        hf.ValueSequence(path="resources.main[1]", values=[0, 1], nesting_order=0)


def test_raise_on_resources_value_sequence_with_label_arg():
    with pytest.raises(ValueError):
        hf.ValueSequence(path="resources.main", values=[0, 1], nesting_order=0, label=1)


def test_inputs_value_sequence_simple_path_attributes():
    path = "inputs.p1"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type == "p1"
    assert seq.normalised_path == "inputs.p1"
    assert seq.normalised_inputs_path == "p1"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == ""
    assert seq.resource_scope is None


def test_inputs_value_sequence_path_attributes():
    path = "inputs.p1.a.b"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type == "p1"
    assert seq.normalised_path == "inputs.p1.a.b"
    assert seq.normalised_inputs_path == "p1.a.b"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == "a.b"
    assert seq.resource_scope is None


def test_inputs_value_sequence_with_path_label_path_attributes():
    path = "inputs.p1[1].a.b"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type == "p1[1]"
    assert seq.normalised_path == "inputs.p1[1].a.b"
    assert seq.normalised_inputs_path == "p1[1].a.b"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == "a.b"
    assert seq.resource_scope is None


def test_inputs_value_sequence_with_arg_label_path_attributes():
    path = "inputs.p1.a.b"
    new_path = "inputs.p1[1].a.b"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0, label=1)
    assert seq.path == new_path
    assert seq.labelled_type == "p1[1]"
    assert seq.normalised_path == "inputs.p1[1].a.b"
    assert seq.normalised_inputs_path == "p1[1].a.b"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == "a.b"
    assert seq.resource_scope is None


def test_resources_value_sequence_path_attributes():
    path = "resources.main.num_cores"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type is None
    assert seq.normalised_path == "resources.main.num_cores"
    assert seq.normalised_inputs_path is None
    assert seq.path_type == "resources"
    assert seq.input_type is None
    assert seq.input_path is None
    assert seq.resource_scope == "main"


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_value_sequence_object_values_during_workflow_init(
    null_config, tmp_path: Path, store: str
):
    p1 = hf.Parameter("p1c")
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=p1)])
    obj = P1(a=101)
    seq = hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)
    values_exp = [P1(a=101, d=None)]

    t1 = hf.Task(
        schema=[s1],
        sequences=[seq],
    )
    # before workflow initialisation:
    assert seq.values == values_exp

    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name="temp",
        store=store,
    )

    with wk.batch_update():
        wk.add_task(t1)
        # after workflow initialisation but before store commit:
        assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp

    # after initialisation and store commit:
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_value_sequence_object_values_class_method_during_workflow_init(
    null_config, tmp_path: Path, store: str
):
    p1 = hf.Parameter("p1c")
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=p1)])
    obj = P1.from_data(b=50, c=51)
    seq = hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)
    values_exp = [P1(a=101, d=None)]

    t1 = hf.Task(
        schema=[s1],
        sequences=[seq],
    )
    # before workflow initialisation:
    assert seq.values == values_exp

    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name="temp",
        store=store,
    )

    with wk.batch_update():
        wk.add_task(t1)
        # after workflow initialisation but before store commit:
        assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp

    # after initialisation and store commit:
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_value_sequence_object_values_named_class_method_during_workflow_init(
    null_config, tmp_path: Path, store: str
):
    p1 = hf.Parameter("p1c")
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=p1)])
    data = {"b": 50, "c": 51}
    seq = hf.ValueSequence(
        path="inputs.p1c", values=[data], nesting_order=0, value_class_method="from_data"
    )
    values_exp = [data]

    t1 = hf.Task(
        schema=[s1],
        sequences=[seq],
    )
    # before workflow initialisation:
    assert seq.values == values_exp

    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name="temp",
        store=store,
    )

    with wk.batch_update():
        wk.add_task(t1)
        # after workflow initialisation but before store commit:
        assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp

    # after initialisation and store commit:
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp


def test_nesting_order_two_seqs_parallel(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test", inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2")]
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"], nesting_order=0),
            hf.ValueSequence("inputs.p2", values=["c", "d"], nesting_order=0),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test",
        tasks=[t1],
        path=tmp_path,
    )
    assert wk.tasks.test.num_elements == 2
    assert wk.tasks.test.elements[0].get("inputs") == {"p1": "a", "p2": "c"}
    assert wk.tasks.test.elements[1].get("inputs") == {"p1": "b", "p2": "d"}


def test_nesting_order_two_seqs_parallel_decimal_equiv(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test", inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2")]
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"], nesting_order=1.0),
            hf.ValueSequence("inputs.p2", values=["c", "d"], nesting_order=1.0),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test",
        tasks=[t1],
        path=tmp_path,
    )
    assert wk.tasks.test.num_elements == 2
    assert wk.tasks.test.elements[0].get("inputs") == {"p1": "a", "p2": "c"}
    assert wk.tasks.test.elements[1].get("inputs") == {"p1": "b", "p2": "d"}


def test_nesting_order_two_seqs_nested(
    null_config,
    tmp_path: Path,
):
    ts = hf.TaskSchema(
        objective="test", inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2")]
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"], nesting_order=0),
            hf.ValueSequence("inputs.p2", values=["c", "d"], nesting_order=1),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test",
        tasks=[t1],
        path=tmp_path,
    )
    assert wk.tasks.test.num_elements == 4
    assert wk.tasks.test.elements[0].get("inputs") == {"p1": "a", "p2": "c"}
    assert wk.tasks.test.elements[1].get("inputs") == {"p1": "a", "p2": "d"}
    assert wk.tasks.test.elements[2].get("inputs") == {"p1": "b", "p2": "c"}
    assert wk.tasks.test.elements[3].get("inputs") == {"p1": "b", "p2": "d"}


def test_nesting_order_two_seqs_default_nesting_order(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test", inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2")]
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"]),
            hf.ValueSequence("inputs.p2", values=["c", "d"]),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test",
        tasks=[t1],
        path=tmp_path,
    )
    assert wk.tasks.test.num_elements == 2
    assert wk.tasks.test.elements[0].get("inputs") == {"p1": "a", "p2": "c"}
    assert wk.tasks.test.elements[1].get("inputs") == {"p1": "b", "p2": "d"}


def test_raise_nesting_order_two_seqs_default_nesting_order(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test", inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2")]
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"]),
            hf.ValueSequence("inputs.p2", values=["c", "d", "e"]),
        ],
    )
    with pytest.raises(ValueError):
        wk = hf.Workflow.from_template_data(
            template_name="test",
            tasks=[t1],
            path=tmp_path,
        )


def test_raise_nesting_order_two_seqs_default_nesting_order_decimal(
    null_config, tmp_path: Path
):
    ts = hf.TaskSchema(
        objective="test", inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2")]
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"], nesting_order=0.0),
            hf.ValueSequence("inputs.p2", values=["c", "d", "e"], nesting_order=0.0),
        ],
    )
    with pytest.raises(ValueError):
        wk = hf.Workflow.from_template_data(
            template_name="test",
            tasks=[t1],
            path=tmp_path,
        )


def test_nesting_order_three_seqs_decimal(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2"), hf.SchemaInput("p3")],
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"], nesting_order=0),
            hf.ValueSequence("inputs.p2", values=["c", "d", "e"], nesting_order=1),
            hf.ValueSequence(
                "inputs.p3", values=["f", "g", "h", "i", "j", "k"], nesting_order=1.5
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test",
        tasks=[t1],
        path=tmp_path,
    )
    assert wk.tasks.test.num_elements == 6
    assert wk.tasks.test.elements[0].get("inputs") == {"p1": "a", "p2": "c", "p3": "f"}
    assert wk.tasks.test.elements[1].get("inputs") == {"p1": "a", "p2": "d", "p3": "g"}
    assert wk.tasks.test.elements[2].get("inputs") == {"p1": "a", "p2": "e", "p3": "h"}
    assert wk.tasks.test.elements[3].get("inputs") == {"p1": "b", "p2": "c", "p3": "i"}
    assert wk.tasks.test.elements[4].get("inputs") == {"p1": "b", "p2": "d", "p3": "j"}
    assert wk.tasks.test.elements[5].get("inputs") == {"p1": "b", "p2": "e", "p3": "k"}


def test_nesting_order_three_seqs_all_decimal(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2"), hf.SchemaInput("p3")],
    )
    t1 = hf.Task(
        schema=ts,
        sequences=[
            hf.ValueSequence("inputs.p1", values=["a", "b"], nesting_order=0.5),
            hf.ValueSequence("inputs.p2", values=["c", "d", "e"], nesting_order=1.2),
            hf.ValueSequence(
                "inputs.p3", values=["f", "g", "h", "i", "j", "k"], nesting_order=1.5
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test",
        tasks=[t1],
        path=tmp_path,
    )
    assert wk.tasks.test.num_elements == 6
    assert wk.tasks.test.elements[0].get("inputs") == {"p1": "a", "p2": "c", "p3": "f"}
    assert wk.tasks.test.elements[1].get("inputs") == {"p1": "a", "p2": "d", "p3": "g"}
    assert wk.tasks.test.elements[2].get("inputs") == {"p1": "a", "p2": "e", "p3": "h"}
    assert wk.tasks.test.elements[3].get("inputs") == {"p1": "b", "p2": "c", "p3": "i"}
    assert wk.tasks.test.elements[4].get("inputs") == {"p1": "b", "p2": "d", "p3": "j"}
    assert wk.tasks.test.elements[5].get("inputs") == {"p1": "b", "p2": "e", "p3": "k"}


@pytest.mark.xfail(
    condition=sys.platform == "darwin",
    raises=requests.exceptions.HTTPError,
    reason=(
        "GHA MacOS runners use the same IP address, so we get rate limited when "
        "retrieving demo data from GitHub."
    ),
)
def test_demo_data_values(null_config):
    name = "text_file.txt"
    assert hf.ValueSequence(
        path="inputs.p1", values=[f"<<demo_data_file:{name}>>"]
    ).values[0] == str(hf.demo_data_cache_dir.joinpath(name))


def test_from_linear_space(null_config):
    seq = hf.ValueSequence.from_linear_space(path="inputs.p1", start=0, stop=1, num=6)
    assert np.allclose(seq.values, [0, 0.2, 0.4, 0.6, 0.8, 1.0])


def test_from_rectangle(null_config):
    kwargs = dict(
        path="inputs.p1",
        start=[0, 0],
        stop=[1, 1],
        num=[2, 2],
    )
    seq_coord_0 = hf.ValueSequence.from_rectangle(**kwargs, coord=0)
    seq_coord_1 = hf.ValueSequence.from_rectangle(**kwargs, coord=1)

    assert np.allclose(seq_coord_0.values, [0, 1, 0, 1])
    assert np.allclose(seq_coord_1.values, [1, 1, 0, 0])


def test_from_rectangle_coord_none(null_config):
    kwargs = dict(
        path="inputs.p1",
        start=[0, 0],
        stop=[1, 1],
        num=[2, 2],
    )
    seq = hf.ValueSequence.from_rectangle(**kwargs)
    assert np.allclose(seq.values, [[0, 1], [1, 1], [0, 0], [1, 0]])


def test_environments_sequence_to_resources(null_config):
    seq = hf.ValueSequence(path="environments.my_env.version", values=[1, 2])
    assert seq.path == "resources.any.environments.my_env.version"
