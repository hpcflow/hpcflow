from textwrap import dedent
import hpcflow.app as hf

import pytest


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_basic_jinja_template(null_config, tmp_path):
    jinja_template_name = "test_template.txt"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("name")),
            hf.SchemaInput(parameter=hf.Parameter("fruits")),
        ],
        actions=[hf.Action(jinja_template=jinja_template_name)],
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
