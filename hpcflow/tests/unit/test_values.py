"""Testing of the ValuesMixin class"""

from pathlib import Path
import sys
from textwrap import dedent

import numpy as np
import pytest
import requests

from hpcflow.sdk.core.utils import read_YAML_str
from hpcflow.app import app as hf


def test_demo_data_paths_resolved_in_input_values():
    assert Path(hf.InputValue("p1", "<<demo_data_file:text_file_1.txt>>").value).is_file()
    assert Path(hf.InputValue("p1", "<<demo_data_file:zip_file.zip>>").value).is_file()


def test_demo_data_paths_resolved_in_input_values_class_methods():
    assert hf.InputValue.from_file("p1", "<<demo_data_file:text_file_1.txt>>").value == [
        str(i) for i in range(1, 11)
    ]


def test_demo_data_paths_resolved_in_input_values_from_yaml():

    es = dedent("""\
    inputs:
      p1: <<demo_data_file:text_file_1.txt>>
      p2: <<demo_data_file:zip_file.zip>>
    """)
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)

    assert Path(es.inputs[0].value).is_file()
    assert Path(es.inputs[1].value).is_file()


def test_demo_data_paths_resolved_in_input_values_class_methods_from_yaml():
    es = dedent("""\
    inputs:
      p1::from_file:
        file_path: <<demo_data_file:text_file_1.txt>>
    """)
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    assert es.inputs[0].value == [str(i) for i in range(1, 11)]


def test_demo_data_paths_resolved_in_value_sequences():
    seqs = hf.ValueSequence(
        "inputs.p1",
        values=["<<demo_data_file:text_file_1.txt>>", "<<demo_data_file:zip_file.zip>>"],
    )
    assert all(Path(val_i).is_file() for val_i in seqs.values)


def test_demo_data_paths_resolved_in_value_sequences_class_methods():
    assert hf.ValueSequence.from_file(
        "inputs.p1",
        file_path="<<demo_data_file:text_file_1.txt>>",
    ).values == [str(i) for i in range(1, 11)]


def test_demo_data_paths_resolved_in_value_sequences_from_yaml():
    es = dedent("""\
    sequences:
      - path: inputs.p1
        values:
          - <<demo_data_file:text_file_1.txt>>
          - <<demo_data_file:zip_file.zip>>
    """)
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    assert all(Path(val_i).is_file() for val_i in es.sequences[0].values)


def test_demo_data_paths_resolved_in_value_sequences_from_yaml_class_methods():
    es = dedent("""\
    sequences:
      - path: inputs.p1
        values::from_file:
          file_path: <<demo_data_file:text_file_1.txt>>
    """)
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    assert es.sequences[0].values == [str(i) for i in range(1, 11)]


def test_value_sequence_from_file_sampled_lines():
    all_lines = [str(i) for i in range(1, 11)]
    seq = hf.ValueSequence.from_file(
        "inputs.p1",
        file_path="<<demo_data_file:text_file_1.txt>>",
        num_samples=4,
        seed=5,
    )
    assert len(seq.values) == 4
    assert all(val in all_lines for val in seq.values)
    assert len(set(seq.values)) == 4  # without replacement
    assert seq._values_method == "from_file"
    assert seq._values_method_args == {
        "file_path": seq._values_method_args["file_path"],
        "num_samples": 4,
        "seed": 5,
        "replace": False,
    }
    # reproducible:
    seq_2 = hf.ValueSequence.from_file(
        "inputs.p1",
        file_path="<<demo_data_file:text_file_1.txt>>",
        num_samples=4,
        seed=5,
    )
    assert seq_2.values == seq.values


def test_value_sequence_from_file_no_sampling_args_recorded_without_num_samples():
    seq = hf.ValueSequence.from_file(
        "inputs.p1", file_path="<<demo_data_file:text_file_1.txt>>"
    )
    assert seq._values_method_args is not None
    assert set(seq._values_method_args) == {"file_path"}


def test_value_sequence_from_file_sampled_lines_from_yaml():
    es = dedent("""\
    sequences:
      - path: inputs.p1
        values::from_file:
          file_path: <<demo_data_file:text_file_1.txt>>
          num_samples: 3
          seed: 1
    """)
    es_JSON = read_YAML_str(es)
    es = hf.ElementSet.from_json_like(es_JSON, shared_data=hf.template_components)
    all_lines = [str(i) for i in range(1, 11)]
    assert len(es.sequences[0].values) == 3
    assert all(val in all_lines for val in es.sequences[0].values)


def test_input_value_from_file_sampled_lines():
    all_lines = [str(i) for i in range(1, 11)]
    inp = hf.InputValue.from_file(
        "p1", "<<demo_data_file:text_file_1.txt>>", num_samples=2, seed=9
    )
    assert len(inp.value) == 2
    assert all(val in all_lines for val in inp.value)
    assert inp._value_method_args is not None
    assert inp._value_method_args["seed"] == 9


def test_value_sequence_from_load_txt_sampled_rows(tmp_path):
    arr = np.arange(30, dtype=float).reshape(10, 3)
    file_path = tmp_path / "arr.txt"
    np.savetxt(file_path, arr)
    seq = hf.ValueSequence.from_load_txt(
        "inputs.p1", file_path=file_path, num_samples=4, seed=2
    )
    assert len(seq.values) == 4
    file_rows = {tuple(row) for row in arr}
    assert all(tuple(row) in file_rows for row in seq.values)
    assert seq._values_method == "from_load_txt"
    assert seq._values_method_args is not None
    assert seq._values_method_args["seed"] == 2
    seq_2 = hf.ValueSequence.from_load_txt(
        "inputs.p1", file_path=file_path, num_samples=4, seed=2
    )
    assert np.array_equal(np.asarray(seq_2.values), np.asarray(seq.values))
