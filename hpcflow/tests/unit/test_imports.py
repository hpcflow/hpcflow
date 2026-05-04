import pytest
import hpcflow.app as hf
from hpcflow.sdk.core.parameters import NullDefault
from hpcflow.sdk.core.test_utils import make_schemas


def test_import_from_json_like(tmp_path):

    (s1,) = make_schemas(({"p1": NullDefault.NULL}, ("p2",), "t1a"))
    wfA = hf.Workflow.from_template_data(
        template_name="test_import_A",
        tasks=[hf.Task(schema=s1, inputs={"p1": 100})],
        path=tmp_path,
    )
    imp_1 = hf.Import.from_json_like(
        {
            "label": "p2",
            "workflow": wfA.path,
            "parameters": [{"parameter": "p2", "as": "p2", "source": "task.t1a.output"}],
        },
        shared_data=hf.app.template_components,
    )
    imp_2 = hf.Import.from_json_like(
        {"label": "p2", "workflow": wfA.path, "parameters": {"p2": "p2"}},
        shared_data=hf.app.template_components,
    )
    imp_3 = hf.Import.from_json_like(
        {
            "label": "p2",
            "workflow": wfA.path,
        },
        shared_data=hf.app.template_components,
    )

    wt_1 = hf.WorkflowTemplate.from_json_like(
        {
            "name": "test_imports",
            "imports": {"p2": {"workflow": wfA.path}},
        },
        shared_data=hf.app.template_components,
    )
    imp_4 = wt_1.imports[0]

    wt_2 = hf.WorkflowTemplate.from_json_like(
        {
            "name": "test_imports",
            "imports": {"p2": {"workflow": wfA.path, "parameters": {"p2": "p2"}}},
        },
        shared_data=hf.app.template_components,
    )
    imp_5 = wt_2.imports[0]

    wt_3 = hf.WorkflowTemplate.from_json_like(
        {
            "name": "test_imports",
            "imports": {
                "p2": {"workflow": wfA.path, "parameters": [{"parameter": "p2"}]}
            },
        },
        shared_data=hf.app.template_components,
    )
    imp_6 = wt_3.imports[0]

    wt_4 = hf.WorkflowTemplate.from_json_like(
        {
            "name": "test_imports",
            "imports": {
                "p2": {
                    "workflow": wfA.path,
                    "parameters": [{"parameter": "p2", "as": "p2"}],
                }
            },
        },
        shared_data=hf.app.template_components,
    )
    imp_7 = wt_4.imports[0]

    imp_8 = hf.Import(
        label="p2",
        workflow=wfA.path,
        parameters=[hf.ImportParameter(hf.Parameter("p2"), source="task.t1a.output")],
    )

    assert imp_1 == imp_2 == imp_3 == imp_4 == imp_5 == imp_6 == imp_7 == imp_8


def test_imports_defined_on_workflow_template(tmp_path):
    (s1, s2) = make_schemas(
        ({"p1": NullDefault.NULL}, ("p2",), "t1a"),
        ({"p2": NullDefault.NULL}, ("p3",), "t1b"),
    )
    wfA = hf.Workflow.from_template_data(
        template_name="test_import_A",
        tasks=[hf.Task(schema=s1, inputs={"p1": 100})],
        path=tmp_path,
    )
    wfB = hf.Workflow.from_template_data(
        template_name="test_import_B",
        imports=[hf.Import("p2", wfA)],
        tasks=[hf.Task(schema=s2)],
        path=tmp_path,
    )
    wfB.template.imports == [
        hf.Import(
            label="p2",
            workflow=wfA,
            parameters=[hf.ImportParameter(parameter="p2", as_="p2")],
        )
    ]


def test_import_source_selected_by_default(tmp_path):
    (s1, s2) = make_schemas(
        ({"p1": NullDefault.NULL}, ("p2",), "t1a"),
        ({"p2": NullDefault.NULL}, ("p3",), "t1b"),
    )
    wfA = hf.Workflow.from_template_data(
        template_name="test_import_A",
        tasks=[hf.Task(schema=s1, inputs={"p1": 100})],
        path=tmp_path,
    )
    wfB = hf.Workflow.from_template_data(
        template_name="test_import_B",
        imports=[hf.Import("p2", wfA)],
        tasks=[hf.Task(schema=s2)],
        path=tmp_path,
    )
    assert wfB.tasks[0].template.element_sets[0].input_sources["p2"] == [
        hf.InputSource.import_(import_ref=0)
    ]


def test_import_source_selected_explicit(tmp_path):
    (s1, s2) = make_schemas(
        ({"p1": NullDefault.NULL}, ("p2",), "t1a"),
        ({"p2": NullDefault.NULL}, ("p3",), "t1b"),
    )
    wfA = hf.Workflow.from_template_data(
        template_name="test_import_A",
        tasks=[hf.Task(schema=s1, inputs={"p1": 100})],
        path=tmp_path,
    )
    wfB = hf.Workflow.from_template_data(
        template_name="test_import_B",
        imports=[hf.Import("p2", wfA)],
        tasks=[hf.Task(schema=s2, input_sources={"p2": [hf.InputSource.import_("p2")]})],
        path=tmp_path,
    )
    assert wfB.tasks[0].template.element_sets[0].input_sources["p2"] == [
        hf.InputSource.import_(import_ref=0)
    ]


@pytest.mark.integration
def test_import_simple(tmp_path):

    s1, s2 = make_schemas(
        ({"p1": NullDefault.NULL}, ("p2",), "t1a"),
        ({"p2": NullDefault.NULL}, ("p3",), "t1b"),
    )

    wfA = hf.Workflow.from_template_data(
        template_name="test_import_A",
        tasks=[hf.Task(schema=s1, inputs={"p1": 100})],
        path=tmp_path,
    )
    wfA.submit(wait=True, status=False)

    p2_wfA_val = wfA.tasks[0].elements[0].get("outputs.p2")

    wfB = hf.Workflow.from_template_data(
        template_name="test_import_B",
        imports=[hf.Import("p2", wfA)],
        tasks=[hf.Task(schema=s2)],
        path=tmp_path,
    )
    assert wfB.tasks[0].elements[0].input_sources["inputs.p2"] == hf.InputSource.import_(
        import_ref=0
    )
    p2_wfB_val = wfB.tasks[0].elements[0].get("inputs.p2")

    assert p2_wfB_val == p2_wfA_val
