import numpy as np
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_schemas, make_workflow
from hpcflow.sdk.submission.jobscript import is_jobscript_array, resolve_jobscript_blocks

import pytest


def test_resolve_jobscript_blocks():
    # separate jobscripts due to `is_array`:
    jobscripts = {
        0: {"is_array": True, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": True, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": True, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": True,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # separate jobscripts due to different `resource_hash`:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 1, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # separate jobscripts due to `is_array`:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": True, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": True,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # separate jobscripts due to `is_array`:
    jobscripts = {
        0: {"is_array": True, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": True, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # combined jobscript due to same resource_hash, not is_array, and dependencies:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        2: {"is_array": False, "resource_hash": 0, "dependencies": {1: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {}},
                {"dependencies": {(0, 0): "DEP_DATA"}},
                {"dependencies": {(0, 1): "DEP_DATA"}},
            ],
        }
    ]

    # combined jobscript due to same resource_hash, not is_array, and dependencies:
    # (checking non-consecutive jobscript index `3` is inconsequential)
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        3: {"is_array": False, "resource_hash": 0, "dependencies": {1: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {}},
                {"dependencies": {(0, 0): "DEP_DATA"}},
                {"dependencies": {(0, 1): "DEP_DATA"}},
            ],
        }
    ]

    # jobscript 0 and 1 combined, not 2 due to independence:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        2: {"is_array": False, "resource_hash": 0, "dependencies": {}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {}}, {"dependencies": {(0, 0): "DEP_DATA"}}],
        },
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
    ]

    # separate jobscripts 0,1 due to independence, separate jobscript 2 due to dependence
    # that spans multiple upstream jobscripts that are independent:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        2: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 1: "DEP_DATA"},
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA", (1, 0): "DEP_DATA"}}],
        },
    ]

    # combine jobscripts due to dependence
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        2: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 1: "DEP_DATA"},
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {}},
                {"dependencies": {(0, 0): "DEP_DATA"}},
                {"dependencies": {(0, 0): "DEP_DATA", (0, 1): "DEP_DATA"}},
            ],
        }
    ]

    # separate jobscripts 0,1 due to independence, combined jobscripts 3,4 due to shared
    # dependencies:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        2: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 1: "DEP_DATA"},
        },
        3: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 1: "DEP_DATA", 2: "DEP_DATA"},
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {(0, 0): "DEP_DATA", (1, 0): "DEP_DATA"}},
                {
                    "dependencies": {
                        (0, 0): "DEP_DATA",
                        (1, 0): "DEP_DATA",
                        (2, 0): "DEP_DATA",
                    }
                },
            ],
        },
    ]

    # seperate jobscripts 0,1,2 due to resource hashes, combined 2,3 due to shared
    # upstream dependencies:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 1, "dependencies": {0: "DEP_DATA"}},
        2: {"is_array": False, "resource_hash": 0, "dependencies": {1: "DEP_DATA"}},
        3: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 2: "DEP_DATA"},
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {(1, 0): "DEP_DATA"}},
                {"dependencies": {(0, 0): "DEP_DATA", (2, 0): "DEP_DATA"}},
            ],
        },
    ]

    # test non-consecutive jobscript indices (i.e. 0,1 merged across tasks in previous
    # step); separate jobscripts 0,2,3 due to resource hashes, combined 3,4 due to shared
    # upstream dependencies:
    jobscripts = {
        0: {"resource_hash": 0, "dependencies": {}, "is_array": False},
        2: {
            "resource_hash": 1,
            "dependencies": {0: "DEP_DATA"},
            "is_array": False,
        },
        3: {
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 2: "DEP_DATA"},
            "is_array": False,
        },
        4: {
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 3: "DEP_DATA"},
            "is_array": False,
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {(0, 0): "DEP_DATA", (1, 0): "DEP_DATA"}},
                {"dependencies": {(0, 0): "DEP_DATA", (2, 0): "DEP_DATA"}},
            ],
        },
    ]


def test_is_job_array_raises_on_bad_scheduler():
    resources = hf.ElementResources(use_job_array=True)
    resources.set_defaults()
    with pytest.raises(ValueError):
        is_jobscript_array(resources=resources, num_elements=2, store=None)


def test_force_array(null_config, tmp_path):
    wk = make_workflow(
        [[{"p1": None}, ("p2",), "t1"]],
        path=tmp_path,
        local_sequences={0: [("inputs.p1", 2, 0)]},
        name="w1",
        overwrite=False,
    )
    sub = wk.add_submission(force_array=True)
    assert len(sub.jobscripts) == 1
    assert sub.jobscripts[0].is_array


def test_merge_jobscript_multi_dependence(null_config, tmp_path):
    s1, s2, s3 = make_schemas(
        ({}, ("p1", "p2"), "t1"),
        (
            {
                "p1": None,
            },
            ("p3",),
            "t2",
        ),
        ({"p1": None, "p3": None}, tuple(), "t3"),
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_merge_js",
        workflow_name="test_merge_js",
        overwrite=True,
        path=tmp_path,
        tasks=[
            hf.Task(schema=s1, repeats=2),
            hf.Task(schema=s2),
            hf.Task(schema=s3),
        ],
    )
    sub = wk.add_submission()
    assert len(sub.jobscripts) == 1
    assert len(sub.jobscripts[0].blocks) == 1


def test_merge_jobscript_multi_dependence_non_array_source(null_config, tmp_path):
    # the second two jobscripts should merge
    s1, s2, s3 = make_schemas(
        ({}, ("p1", "p2"), "t1"),
        (
            {
                "p1": None,
            },
            ("p3",),
            "t2",
        ),
        ({"p1": None, "p3": None}, tuple(), "t3"),
    )
    wk = hf.Workflow.from_template_data(
        template_name="wk_test_merge",
        path=tmp_path,
        tasks=[
            hf.Task(schema=s1),
            hf.Task(schema=s2, repeats=2),
            hf.Task(schema=s3),
        ],
    )
    sub = wk.add_submission(force_array=True)

    assert len(sub.jobscripts) == 2
    assert len(sub.jobscripts[0].blocks) == 1
    assert len(sub.jobscripts[1].blocks) == 1


def test_multi_block_jobscript_multi_dependence(null_config, tmp_path):

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2", "p3"), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
        ({"p3": None, "p5": None}, (), "t4"),
    )
    tasks = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk = hf.Workflow.from_template_data(
        template_name="test_js_blocks",
        workflow_name="test_js_blocks",
        tasks=tasks,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert len(sub.jobscripts) == 1
    assert len(sub.jobscripts[0].blocks) == 1


def test_multi_block_jobscript_multi_dependence_distinct_resources(null_config, tmp_path):

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2", "p3"), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
        ({"p3": None, "p5": None}, (), "t4"),
    )
    tasks = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk = hf.Workflow.from_template_data(
        template_name="test_js_blocks",
        workflow_name="test_js_blocks",
        tasks=tasks,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert len(sub.jobscripts) == 3
    assert len(sub.jobscripts[0].blocks) == 1
    assert len(sub.jobscripts[1].blocks) == 1
    assert len(sub.jobscripts[2].blocks) == 2


def test_multi_block_jobscript_multi_dependence_distinct_resources_sequence_and_group(
    null_config, tmp_path
):

    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
    )
    s4 = hf.TaskSchema(
        objective="t4",
        inputs=[hf.SchemaInput("p2", group="g1"), hf.SchemaInput("p5", group="g1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command("echo $((<<sum(parameter:p2)>> + <<sum(parameter:p5)>>))")
                ]
            )
        ],
    )
    tasks = [
        hf.Task(
            schema=s1,
            sequences=[hf.ValueSequence(path="inputs.p1", values=[1, 2])],
            groups=[hf.ElementGroup(name="g1")],
        ),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3, groups=[hf.ElementGroup(name="g1")]),
        hf.Task(schema=s4),
    ]
    wk = hf.Workflow.from_template_data(
        template_name="test_js_blocks",
        workflow_name="test_js_blocks",
        tasks=tasks,
        overwrite=True,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert len(sub.jobscripts) == 3
    assert len(sub.jobscripts[0].blocks) == 1
    assert len(sub.jobscripts[1].blocks) == 1
    assert len(sub.jobscripts[2].blocks) == 2


def test_combine_scripts_unset_False_jobscript_hash_equivalence(null_config, tmp_path):

    s1 = hf.TaskSchema(
        objective="t1",
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
            hf.Action(commands=[hf.Command(command='echo "hello!"')]),
        ],
    )
    t1 = hf.Task(schema=s1)
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        resources={
            "any": {
                "combine_scripts": False,  # only applies to the Python script action
            },
        },
        template_name="combine_scripts_test",
        path=tmp_path,
    )
    sub = wk.add_submission()

    # test that even though `combine_scripts` is not set on second action (because it is
    # not a Python script action), the resources have an equivalent hash and thus only one
    # jobscript is generated:

    iter_1 = wk.tasks.t1.elements[0].iterations[0]
    act_1 = iter_1.action_runs[0].action
    act_2 = iter_1.action_runs[1].action

    res_1 = iter_1.get_resources_obj(act_1)
    res_2 = iter_1.get_resources_obj(act_2)

    # set to False on first action:
    assert iter_1.get_resources_obj(act_1).combine_scripts == False

    # not set on second action:
    assert iter_1.get_resources_obj(act_2).combine_scripts == None

    # hashes equivalent:
    assert res_1.get_jobscript_hash() == res_2.get_jobscript_hash()

    assert len(sub.jobscripts) == 1


def test_JS_parallelism_default_zarr(null_config, tmp_path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_JS_parallelism_default_set_zarr",
        path=tmp_path,
        tasks=[t1],
        store="zarr",
    )

    wk.add_submission()  # do not set JS_parallelism

    # zarr supports JS parallelism, so by default should be set to "scheduled":
    assert wk.submissions[0].JS_parallelism == "scheduled"


def test_JS_parallelism_default_json(null_config, tmp_path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_JS_parallelism_default_set_json",
        path=tmp_path,
        tasks=[t1],
        store="json",
    )

    wk.add_submission()  # do not set JS_parallelism

    # json does not support JS parallelism, so by default should be set to False:
    assert wk.submissions[0].JS_parallelism is False


def test_jobscript_block_run_IDs_equivalence_JSON_Zarr(null_config, tmp_path):
    """The zarr store keeps jobscript-block run IDs in separate arrays, so test
    equivalence."""

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2", "p3"), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
        ({"p3": None, "p5": None}, (), "t4"),
    )
    tasks_zarr = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_zarr = hf.Workflow.from_template_data(
        template_name="test_js_blocks_zarr",
        tasks=tasks_zarr,
        path=tmp_path,
        store="zarr",
    )
    sub_zarr = wk_zarr.add_submission()

    tasks_json = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_json = hf.Workflow.from_template_data(
        template_name="test_js_blocks_json",
        tasks=tasks_json,
        path=tmp_path,
        store="json",
    )
    sub_json = wk_json.add_submission()

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        js_json = sub_json.jobscripts[js_idx]
        assert np.array_equal(js_zarr.all_EAR_IDs, js_json.all_EAR_IDs)

    # reload both workflows from disk, and check again, since above will check data from
    # in-memory modified Submission object
    sub_json = wk_json.reload().submissions[0]
    sub_zarr = wk_zarr.reload().submissions[0]

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        js_json = sub_json.jobscripts[js_idx]
        assert np.array_equal(js_zarr.all_EAR_IDs, js_json.all_EAR_IDs)


def test_jobscript_task_element_maps_equivalence_JSON_Zarr(null_config, tmp_path):
    """The zarr store keeps jobscript-block task-element maps in separate arrays, so test
    equivalence."""

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2", "p3"), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
        ({"p3": None, "p5": None}, (), "t4"),
    )
    tasks_zarr = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_zarr = hf.Workflow.from_template_data(
        template_name="test_js_blocks_zarr",
        tasks=tasks_zarr,
        path=tmp_path,
        store="zarr",
    )
    sub_zarr = wk_zarr.add_submission()

    tasks_json = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_json = hf.Workflow.from_template_data(
        template_name="test_js_blocks_json",
        tasks=tasks_json,
        path=tmp_path,
        store="json",
    )
    sub_json = wk_json.add_submission()

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        assert len(js_zarr.blocks) == len(sub_json.jobscripts[js_idx].blocks)
        for blk_idx, js_blk_zarr in enumerate(js_zarr.blocks):
            js_blk_json = sub_json.jobscripts[js_idx].blocks[blk_idx]
            assert js_blk_zarr.task_elements == js_blk_json.task_elements

    # reload both workflows from disk, and check again, since above will check data from
    # in-memory modified Submission object
    sub_json = wk_json.reload().submissions[0]
    sub_zarr = wk_zarr.reload().submissions[0]

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        assert len(js_zarr.blocks) == len(sub_json.jobscripts[js_idx].blocks)
        for blk_idx, js_blk_zarr in enumerate(js_zarr.blocks):
            js_blk_json = sub_json.jobscripts[js_idx].blocks[blk_idx]
            assert js_blk_zarr.task_elements == js_blk_json.task_elements


def test_jobscript_task_actions_equivalence_JSON_Zarr(null_config, tmp_path):
    """The zarr store keeps jobscript-block task-actions in separate arrays, so test
    equivalence."""

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2", "p3"), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
        ({"p3": None, "p5": None}, (), "t4"),
    )
    tasks_zarr = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_zarr = hf.Workflow.from_template_data(
        template_name="test_js_blocks_zarr",
        tasks=tasks_zarr,
        path=tmp_path,
        store="zarr",
    )
    sub_zarr = wk_zarr.add_submission()

    tasks_json = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_json = hf.Workflow.from_template_data(
        template_name="test_js_blocks_json",
        tasks=tasks_json,
        path=tmp_path,
        store="json",
    )
    sub_json = wk_json.add_submission()

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        assert len(js_zarr.blocks) == len(sub_json.jobscripts[js_idx].blocks)
        for blk_idx, js_blk_zarr in enumerate(js_zarr.blocks):
            js_blk_json = sub_json.jobscripts[js_idx].blocks[blk_idx]
            assert np.array_equal(js_blk_zarr.task_actions, js_blk_json.task_actions)

    # reload both workflows from disk, and check again, since above will check data from
    # in-memory modified Submission object
    sub_json = wk_json.reload().submissions[0]
    sub_zarr = wk_zarr.reload().submissions[0]

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        assert len(js_zarr.blocks) == len(sub_json.jobscripts[js_idx].blocks)
        for blk_idx, js_blk_zarr in enumerate(js_zarr.blocks):
            js_blk_json = sub_json.jobscripts[js_idx].blocks[blk_idx]
            assert np.array_equal(js_blk_zarr.task_actions, js_blk_json.task_actions)


def test_jobscript_dependencies_equivalence_JSON_Zarr(null_config, tmp_path):
    """The zarr store keeps jobscript-block dependencies in separate arrays, so test
    equivalence."""

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2", "p3"), "t1"),
        ({"p2": None}, ("p4",), "t2"),
        ({"p4": None}, ("p5",), "t3"),
        ({"p3": None, "p5": None}, (), "t4"),
    )
    tasks_zarr = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_zarr = hf.Workflow.from_template_data(
        template_name="test_js_blocks_zarr",
        tasks=tasks_zarr,
        path=tmp_path,
        store="zarr",
    )
    sub_zarr = wk_zarr.add_submission()

    tasks_json = [
        hf.Task(schema=s1, inputs={"p1": 101}),
        hf.Task(schema=s2, resources={"any": {"num_cores": 2}}),
        hf.Task(schema=s3),
        hf.Task(schema=s4),
    ]
    wk_json = hf.Workflow.from_template_data(
        template_name="test_js_blocks_json",
        tasks=tasks_json,
        path=tmp_path,
        store="json",
    )
    sub_json = wk_json.add_submission()

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        assert len(js_zarr.blocks) == len(sub_json.jobscripts[js_idx].blocks)
        for blk_idx, js_blk_zarr in enumerate(js_zarr.blocks):
            js_blk_json = sub_json.jobscripts[js_idx].blocks[blk_idx]
            assert js_blk_zarr.dependencies == js_blk_json.dependencies

    # reload both workflows from disk, and check again, since above will check data from
    # in-memory modified Submission object
    sub_json = wk_json.reload().submissions[0]
    sub_zarr = wk_zarr.reload().submissions[0]

    assert len(sub_zarr.jobscripts) == len(sub_json.jobscripts)

    for js_idx, js_zarr in enumerate(sub_zarr.jobscripts):
        assert len(js_zarr.blocks) == len(sub_json.jobscripts[js_idx].blocks)
        for blk_idx, js_blk_zarr in enumerate(js_zarr.blocks):
            js_blk_json = sub_json.jobscripts[js_idx].blocks[blk_idx]
            assert js_blk_zarr.dependencies == js_blk_json.dependencies
