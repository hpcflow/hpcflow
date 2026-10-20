from __future__ import annotations
from pathlib import Path
from textwrap import dedent

import numpy as np
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_schemas
from hpcflow.sdk.core.utils import get_file_context


def test_MPS_sequences():
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    assert mps.sequences == [
        hf.ValueSequence(path="inputs.p1", values=[0, 1]),
        hf.ValueSequence(path="inputs.p2", values=[2, 3]),
    ]


def test_MPS_sequences_moved_to_element_set():
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    es = hf.ElementSet(multi_path_sequences=[mps])
    expected_mps_seqs = [
        hf.ValueSequence(path="inputs.p1", values=[0, 1]),
        hf.ValueSequence(path="inputs.p2", values=[2, 3]),
    ]
    assert mps._sequences is None
    assert mps.sequence_indices == [0, 2]
    assert es.sequences == mps.sequences
    assert es.sequences == expected_mps_seqs


def test_MPS_sequences_moved_to_element_set_with_existing_sequences():
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    seq = hf.ValueSequence(path="inputs.p0", values=[0, 1])
    expected_mps_seqs = [
        hf.ValueSequence(path="inputs.p1", values=[0, 1]),
        hf.ValueSequence(path="inputs.p2", values=[2, 3]),
    ]
    es = hf.ElementSet(
        sequences=[seq],
        multi_path_sequences=[mps],
    )
    assert mps._sequences is None
    assert mps.sequence_indices == [1, 3]
    assert mps.sequences == expected_mps_seqs
    assert es.sequences == [seq, *expected_mps_seqs]


def test_MPS_sequences_moved_to_task_element_set():
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    s1 = make_schemas(({"p1": None, "p2": None}, ()))
    t1 = hf.Task(s1, multi_path_sequences=[mps])
    expected_mps_seqs = [
        hf.ValueSequence(path="inputs.p1", values=[0, 1]),
        hf.ValueSequence(path="inputs.p2", values=[2, 3]),
    ]
    es = t1.element_sets[0]
    assert mps._sequences is None
    assert mps.sequence_indices == [0, 2]
    assert es.sequences == mps.sequences
    assert es.sequences == expected_mps_seqs


def test_MPS_sequences_moved_to_task_element_set_with_existing_sequences():
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    seq = hf.ValueSequence(path="inputs.p0", values=[0, 1])
    s1 = make_schemas(({"p0": None, "p1": None, "p2": None}, ()))
    t1 = hf.Task(s1, sequences=[seq], multi_path_sequences=[mps])
    expected_mps_seqs = [
        hf.ValueSequence(path="inputs.p1", values=[0, 1]),
        hf.ValueSequence(path="inputs.p2", values=[2, 3]),
    ]
    es = t1.element_sets[0]
    assert mps._sequences is None
    assert mps.sequence_indices == [1, 3]
    assert mps.sequences == expected_mps_seqs
    assert es.sequences == [seq, *expected_mps_seqs]


def test_MPS_sequence_element_inputs(tmp_path):
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    s1 = make_schemas(({"p1": None, "p2": None}, ()))
    t1 = hf.Task(s1, multi_path_sequences=[mps])
    wf = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_multi_path_sequence",
        path=tmp_path,
    )
    assert len(wf.template.tasks[0].element_sets[0].sequences) == 2
    assert wf.tasks[0].num_elements == 2
    assert wf.tasks[0].elements[0].get("inputs") == {"p1": 0, "p2": 2}
    assert wf.tasks[0].elements[1].get("inputs") == {"p1": 1, "p2": 3}


def test_MPS_sequence_element_inputs_with_existing_sequence(tmp_path):
    mps = hf.MultiPathSequence(paths=("inputs.p1", "inputs.p2"), values=[[0, 1], [2, 3]])
    seq = hf.ValueSequence(path="inputs.p0", values=[0, 1])
    s1 = make_schemas(({"p0": None, "p1": None, "p2": None}, ()))
    t1 = hf.Task(s1, sequences=[seq], multi_path_sequences=[mps])
    wf = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_multi_path_sequence",
        path=tmp_path,
    )
    assert len(wf.template.tasks[0].element_sets[0].sequences) == 3
    assert wf.tasks[0].num_elements == 2
    assert wf.tasks[0].elements[0].get("inputs") == {"p0": 0, "p1": 0, "p2": 2}
    assert wf.tasks[0].elements[1].get("inputs") == {"p0": 1, "p1": 1, "p2": 3}

    # check the same on reload:
    wf = wf.reload()
    assert len(wf.template.tasks[0].element_sets[0].sequences) == 3
    assert wf.tasks[0].num_elements == 2
    assert wf.tasks[0].elements[0].get("inputs") == {"p0": 0, "p1": 0, "p2": 2}
    assert wf.tasks[0].elements[1].get("inputs") == {"p0": 1, "p1": 1, "p2": 3}


@pytest.mark.integration
def test_MPS_element_outputs(tmp_path):
    with get_file_context("hpcflow.tests.data", "multi_path_sequences.yaml") as file_path:
        wf = hf.make_and_submit_workflow(
            file_path,
            path=tmp_path,
            status=False,
            add_to_known=False,
            wait=True,
        )
        assert wf.tasks[0].num_elements == 2

        p2 = wf.tasks[0].elements[0].outputs.p2
        assert isinstance(p2, hf.ElementParameter)
        assert p2.value == 302

        p2 = wf.tasks[0].elements[1].outputs.p2
        assert isinstance(p2, hf.ElementParameter)
        assert p2.value == 304


def test_MPS_latin_hypercube_sequence_values():
    wft_yaml = dedent("""\
        name: test_latin_hypercube_sampling
        template_components:
          task_schemas:
            - objective: define_p1
              inputs:
                - parameter: p1
        tasks:
          - schema: define_p1
            inputs:
              p1: {}
            multi_path_sequences:
              - paths: [inputs.p1.a, inputs.p1.b]
                values::from_latin_hypercube:
                  num_samples: 5
    """)
    wft = hf.WorkflowTemplate.from_YAML_string(wft_yaml)
    es = wft.tasks[0].element_sets[0]
    assert len(es.multi_path_sequences) == 1
    mps_values = np.asarray(es.multi_path_sequences[0].values)
    assert mps_values.shape == (2, 5)
    assert len(es.sequences) == 2
    seq_1 = es.sequences[0]
    seq_2 = es.sequences[1]
    assert seq_1.path == "inputs.p1.a"
    assert seq_2.path == "inputs.p1.b"
    assert np.array_equal(np.asarray(seq_1.values), mps_values[0])
    assert np.array_equal(np.asarray(seq_2.values), mps_values[1])


def test_MPS_latin_hypercube_sequence_bounds():

    bounds = {
        "inputs.a": {"extent": [16789.2, 17812.5], "scaling": "linear"},
        "inputs.c": {"extent": [1.0e-10, 1.0e-5], "scaling": "log"},
    }

    mps = hf.MultiPathSequence.from_latin_hypercube(
        paths=["inputs.a", "inputs.b", "inputs.c"],
        num_samples=10,
        bounds=bounds,
    )

    vals_arr = np.array(mps.values)

    assert vals_arr.shape == (3, 10)

    vals_a = vals_arr[0]
    vals_b = vals_arr[1]
    vals_c = vals_arr[2]

    extent_a = bounds["inputs.a"]["extent"]
    extent_b = [0.0, 1.0]
    extent_c = bounds["inputs.c"]["extent"]

    assert np.logical_and(vals_a > extent_a[0], vals_a < extent_a[1]).all()
    assert np.logical_and(vals_b > extent_b[0], vals_b < extent_b[1]).all()
    assert np.logical_and(vals_c > extent_c[0], vals_c < extent_c[1]).all()


def test_MPS_move_from_sequences_list():
    wft_yaml = dedent("""\
        name: test_latin_hypercube_sampling
        template_components:
          task_schemas:
            - objective: define_p1_p2_p3_p4
              inputs:
                - parameter: p1
                - parameter: p2
                - parameter: p3
                - parameter: p4
        tasks:
          - schema: define_p1_p2_p3_p4
            inputs:
              p1: {}
              p2: {}
              p3: {}
            
            multi_path_sequences:
              - paths: [inputs.p1.a, inputs.p1.b]
                values::from_latin_hypercube:
                  num_samples: 4             
            
            sequences:
              - paths: [inputs.p2.a, inputs.p2.b] # actually a multi-path sequence
                values::from_latin_hypercube:
                  num_samples: 4
            
              - path: inputs.p4 # a normal sequence
                values: [0, 1, 2, 3]
            
              - paths: [inputs.p3.a, inputs.p3.b] # actually a multi-path sequence
                values::from_latin_hypercube:
                  num_samples: 4
    """)
    wft = hf.WorkflowTemplate.from_YAML_string(wft_yaml)
    es = wft.tasks[0].element_sets[0]
    mps_lst = es.multi_path_sequences
    seq_lst = es.sequences
    assert len(mps_lst) == 3
    assert len(seq_lst) == 7  # one original plus three multi-path with two paths each

    # check ordering of multi-path sequences is preserved:
    assert mps_lst[0].paths == ["inputs.p1.a", "inputs.p1.b"]
    assert mps_lst[1].paths == ["inputs.p2.a", "inputs.p2.b"]
    assert mps_lst[2].paths == ["inputs.p3.a", "inputs.p3.b"]

    # check sensible ordering of sequences:
    assert seq_lst[0].path == "inputs.p4"
    assert seq_lst[1].path == "inputs.p1.a"
    assert seq_lst[2].path == "inputs.p1.b"
    assert seq_lst[3].path == "inputs.p2.a"
    assert seq_lst[4].path == "inputs.p2.b"
    assert seq_lst[5].path == "inputs.p3.a"
    assert seq_lst[6].path == "inputs.p3.b"


@pytest.fixture
def txt_file(tmp_path: Path) -> Path:
    """A four-column text file with a header comment, like a posterior-samples dump."""
    rng = np.random.default_rng(0)
    arr = rng.random((20, 4))
    file_path = tmp_path / "samples.txt"
    np.savetxt(file_path, arr, header="a, b, c, d")
    return file_path


def test_MPS_from_load_txt_all_rows(txt_file: Path):
    arr = np.loadtxt(txt_file)
    mps = hf.MultiPathSequence.from_load_txt(
        paths=["inputs.p1.a", "inputs.p1.b", "inputs.p1.c", "inputs.p1.d"],
        file_path=txt_file,
    )
    assert np.array_equal(np.asarray(mps.values), arr.T)
    assert mps._values_method == "from_load_txt"
    assert mps._values_method_args is not None
    assert "seed" not in mps._values_method_args  # no sampling, so no seed recorded


def test_MPS_from_load_txt_sampled_rows(txt_file: Path):
    arr = np.loadtxt(txt_file)
    paths = ["inputs.p1.a", "inputs.p1.b", "inputs.p1.c", "inputs.p1.d"]
    mps = hf.MultiPathSequence.from_load_txt(
        paths=paths, file_path=txt_file, num_samples=5, seed=123
    )
    vals = np.asarray(mps.values)
    assert vals.shape == (4, 5)

    # each sampled "element" must be an unmodified row of the file:
    rows = vals.T
    file_rows = {tuple(row) for row in arr}
    assert all(tuple(row) in file_rows for row in rows)

    # without replacement, rows are distinct:
    assert len({tuple(row) for row in rows}) == 5

    # seed is recorded, and reproducible:
    assert mps._values_method_args is not None
    assert mps._values_method_args["seed"] == 123
    assert mps._values_method_args["num_samples"] == 5
    mps_2 = hf.MultiPathSequence.from_load_txt(
        paths=paths, file_path=txt_file, num_samples=5, seed=123
    )
    assert np.array_equal(np.asarray(mps_2.values), vals)


def test_MPS_from_load_txt_sampled_rows_seed_generated_if_unspecified(txt_file: Path):
    paths = ["inputs.p1.a", "inputs.p1.b", "inputs.p1.c", "inputs.p1.d"]
    mps = hf.MultiPathSequence.from_load_txt(
        paths=paths, file_path=txt_file, num_samples=3
    )
    assert mps._values_method_args is not None
    seed = mps._values_method_args["seed"]
    assert isinstance(seed, int)
    mps_2 = hf.MultiPathSequence.from_load_txt(
        paths=paths, file_path=txt_file, num_samples=3, seed=seed
    )
    assert np.array_equal(np.asarray(mps_2.values), np.asarray(mps.values))


def test_MPS_from_load_txt_usecols(txt_file: Path):
    arr = np.loadtxt(txt_file)
    mps = hf.MultiPathSequence.from_load_txt(
        paths=["inputs.p1.a", "inputs.p1.d"], file_path=txt_file, usecols=[0, 3]
    )
    assert np.array_equal(np.asarray(mps.values), arr[:, [0, 3]].T)


def test_MPS_from_load_txt_raises_on_wrong_number_of_paths(txt_file: Path):
    with pytest.raises(ValueError, match="number of columns"):
        hf.MultiPathSequence.from_load_txt(
            paths=["inputs.p1.a", "inputs.p1.b"], file_path=txt_file
        )


def test_MPS_from_load_txt_raises_on_too_many_samples_without_replacement(
    txt_file: Path,
):
    with pytest.raises(ValueError, match="without replacement"):
        hf.MultiPathSequence.from_load_txt(
            paths=["inputs.p1.a", "inputs.p1.b", "inputs.p1.c", "inputs.p1.d"],
            file_path=txt_file,
            num_samples=100,
        )


def test_MPS_from_load_txt_with_replacement_allows_more_samples_than_rows(
    txt_file: Path,
):
    mps = hf.MultiPathSequence.from_load_txt(
        paths=["inputs.p1.a", "inputs.p1.b", "inputs.p1.c", "inputs.p1.d"],
        file_path=txt_file,
        num_samples=100,
        replace=True,
        seed=1,
    )
    assert np.asarray(mps.values).shape == (4, 100)


def test_MPS_from_load_txt_from_yaml_sampled_rows_into_list_items(
    txt_file: Path, tmp_path: Path
):
    """Sampled columns can be set as items of list-valued sub-parameters, and as keys
    that are not valid Python identifiers."""
    arr = np.loadtxt(txt_file)
    wft_yaml = dedent(f"""\
        name: test_MPS_from_load_txt
        template_components:
          task_schemas:
            - objective: define_p1
              inputs:
                - parameter: p1
        tasks:
          - schema: define_p1
            inputs:
              p1:
                a: 1.0
                b: [2.0]
                c: [3.0]
                d-e: 4.0
            multi_path_sequences:
              - paths: [inputs.p1.a, inputs.p1.b.0, inputs.p1.c.0, inputs.p1.d-e]
                values::from_load_txt:
                  file_path: {txt_file.as_posix()}
                  num_samples: 3
                  seed: 7
    """)
    wft = hf.WorkflowTemplate.from_YAML_string(wft_yaml)
    es = wft.tasks[0].element_sets[0]
    assert len(es.multi_path_sequences) == 1
    assert len(es.sequences) == 4
    mps = es.multi_path_sequences[0]
    assert mps._values_method == "from_load_txt"
    assert mps._values_method_args is not None
    assert mps._values_method_args["seed"] == 7
    vals = np.asarray(mps.values)
    assert vals.shape == (4, 3)

    wf = hf.Workflow.from_template(wft, path=tmp_path)
    assert wf.tasks[0].num_elements == 3
    file_rows = {tuple(row) for row in arr}
    for elem_idx, elem in enumerate(wf.tasks[0].elements):
        p1 = elem.get("inputs.p1")
        assert p1 == {
            "a": vals[0, elem_idx],
            "b": [vals[1, elem_idx]],
            "c": [vals[2, elem_idx]],
            "d-e": vals[3, elem_idx],
        }
        assert (p1["a"], p1["b"][0], p1["c"][0], p1["d-e"]) in file_rows
