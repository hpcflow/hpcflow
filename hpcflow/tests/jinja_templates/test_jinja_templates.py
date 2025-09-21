import os
from textwrap import dedent
import hpcflow.app as hf

import pytest


@pytest.mark.integration
def test_basic_jinja_template(null_config, tmp_path):
    jinja_template_name = "test_template.txt"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("name")),
            hf.SchemaInput(parameter=hf.Parameter("fruits")),
        ],
        actions=[hf.Action(jinja_template=f"test/{jinja_template_name}")],
    )
    t1 = hf.Task(schema=s1, inputs={"name": "George", "fruits": ["apple", "orange"]})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="jinja_template_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    run_dir = wk.get_all_EARs()[0].get_directory()
    rendered = run_dir.joinpath(jinja_template_name)

    expected = dedent(
        """\
        Hola, George!

        This is a template, with a loop. Here are your specified fruits:
        - apple
        - orange
        """
    )

    assert rendered.is_file()
    assert rendered.read_text() == expected


@pytest.mark.integration
def test_jinja_template_path_with_resource_var(null_config, tmp_path):
    jinja_template_name = "test_template.txt"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("name")),
            hf.SchemaInput(parameter=hf.Parameter("fruits")),
        ],
        actions=[
            hf.Action(jinja_template=f"<<resource:resources_id>>/{jinja_template_name}")
        ],
    )
    t1 = hf.Task(schema=s1, inputs={"name": "George", "fruits": ["apple", "orange"]})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="jinja_template_test",
        path=tmp_path,
        resources={"any": {"resources_id": "test"}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    run_dir = wk.get_all_EARs()[0].get_directory()
    rendered = run_dir.joinpath(jinja_template_name)

    expected = dedent(
        """\
        Hola, George!

        This is a template, with a loop. Here are your specified fruits:
        - apple
        - orange
        """
    )

    assert rendered.is_file()
    assert rendered.read_text() == expected


@pytest.mark.integration
def test_builtin_jinja_template_path_with_env_var(new_null_config, tmp_path):
    env = hf.Environment(
        name="null_env",
        specifiers={"key": "test"},  # reference this specifier in the jinja template path
        executables=[],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    jinja_template_name = "test_template.txt"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("name")),
            hf.SchemaInput(parameter=hf.Parameter("fruits")),
        ],
        actions=[hf.Action(jinja_template=f"<<env:key>>/{jinja_template_name}")],
    )
    t1 = hf.Task(schema=s1, inputs={"name": "George", "fruits": ["apple", "orange"]})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="jinja_template_test",
        path=tmp_path,
        environments={"null_env": {"key": "test"}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    run_dir = wk.get_all_EARs()[0].get_directory()
    rendered = run_dir.joinpath(jinja_template_name)

    expected = dedent(
        """\
        Hola, George!

        This is a template, with a loop. Here are your specified fruits:
        - apple
        - orange
        """
    )

    assert rendered.is_file()
    assert rendered.read_text() == expected

    hf.reload_template_components()  # remove extra env


@pytest.mark.integration
def test_builtin_jinja_template_path_with_param_var(new_null_config, tmp_path):
    jinja_template_name = "test_template.txt"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("name")),
            hf.SchemaInput(parameter=hf.Parameter("fruits")),
            hf.SchemaInput(parameter=hf.Parameter("key")),
        ],
        actions=[hf.Action(jinja_template=f"<<parameter:key>>/{jinja_template_name}")],
    )
    t1 = hf.Task(
        schema=s1, inputs={"name": "George", "fruits": ["apple", "orange"], "key": "test"}
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="jinja_template_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    run_dir = wk.get_all_EARs()[0].get_directory()
    rendered = run_dir.joinpath(jinja_template_name)

    expected = dedent(
        """\
        Hola, George!

        This is a template, with a loop. Here are your specified fruits:
        - apple
        - orange
        """
    )

    assert rendered.is_file()
    assert rendered.read_text() == expected
