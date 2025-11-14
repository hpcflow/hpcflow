"""Testing of the ValuesMixin class"""

from pathlib import Path
from textwrap import dedent

from hpcflow.sdk.core.utils import read_YAML_str
from hpcflow.app import app as hf


def test_demo_data_paths_resolved_in_input_values(null_config):
    assert Path(hf.InputValue("p1", "<<demo_data_file:text_file.txt>>").value).is_file()
    assert Path(hf.InputValue("p1", "<<demo_data_file:zip_file.zip>>").value).is_file()


def test_demo_data_paths_resolved_in_input_values_class_methods(null_config):
    assert hf.InputValue.from_file("p1", "<<demo_data_file:text_file.txt>>").value == [
        str(i) for i in range(1, 11)
    ]


def test_demo_data_paths_resolved_in_input_values_from_yaml(null_config):

    es = dedent(
        """\
    inputs:
      p1: <<demo_data_file:text_file.txt>>
      p2: <<demo_data_file:zip_file.zip>>
    """
    )
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)

    assert Path(es.inputs[0].value).is_file()
    assert Path(es.inputs[1].value).is_file()


def test_demo_data_paths_resolved_in_input_values_class_methods_from_yaml(null_config):
    es = dedent(
        """\
    inputs:
      p1::from_file:
        file_path: <<demo_data_file:text_file.txt>>
    """
    )
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    assert es.inputs[0].value == [str(i) for i in range(1, 11)]


def test_demo_data_paths_resolved_in_value_sequences(null_config):
    seqs = hf.ValueSequence(
        "inputs.p1",
        values=["<<demo_data_file:text_file.txt>>", "<<demo_data_file:zip_file.zip>>"],
    )
    assert all(Path(val_i).is_file() for val_i in seqs.values)


def test_demo_data_paths_resolved_in_value_sequences_class_methods(null_config):
    assert hf.ValueSequence.from_file(
        "inputs.p1",
        file_path="<<demo_data_file:text_file.txt>>",
    ).values == [str(i) for i in range(1, 11)]


def test_demo_data_paths_resolved_in_value_sequences_from_yaml(null_config):
    es = dedent(
        """\
    sequences:
      - path: inputs.p1
        values:
          - <<demo_data_file:text_file.txt>>
          - <<demo_data_file:zip_file.zip>>
    """
    )
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    assert all(Path(val_i).is_file() for val_i in es.sequences[0].values)


def test_demo_data_paths_resolved_in_value_sequences_from_yaml_class_methods(null_config):
    es = dedent(
        """\
    sequences:
      - path: inputs.p1
        values::from_file:
          file_path: <<demo_data_file:text_file.txt>>
    """
    )
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    assert es.sequences[0].values == [str(i) for i in range(1, 11)]
