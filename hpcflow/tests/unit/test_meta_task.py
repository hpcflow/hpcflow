from textwrap import dedent

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.config.errors import UnknownMetaTaskConstitutiveSchema


def test_basic_meta_task_workflow(new_null_config, tmp_path):
    wk_yaml = dedent(
        """\
        name: test_meta_task
        template_components:
          task_schemas:
            - objective: s0
              inputs: 
                - parameter: p1
              outputs:
                - parameter: p2
              actions:
                - commands:
                  - command: echo "$((<<parameter:p1>> + 1))"
                    stdout: <<int(parameter:p2)>>
  
            - objective: s1
              inputs: 
                - parameter: p2
                - parameter: p2b
              outputs:
                - parameter: p3
              actions:
                - commands:
                  - command: echo "$((<<parameter:p2>> + <<parameter:p2b>>))"
                    stdout: <<int(parameter:p3)>>
  
            - objective: s2
              inputs: 
                - parameter: p3
              outputs:
                - parameter: p4
              actions:
                - commands:
                  - command: echo "$((<<parameter:p3>> + 1))"
                    stdout: <<int(parameter:p4)>>
  
            - objective: s3
              inputs: 
                - parameter: p4
              outputs:
                - parameter: p5
              actions:
                - commands:
                  - command: echo "$((<<parameter:p4>> + 1))"
                    stdout: <<int(parameter:p5)>>

          meta_task_schemas:
            - objective: system_analysis
              inputs:
                - parameter: p2
              outputs:
                - parameter: p4

        meta_tasks:
          system_analysis:
            - schema: s1
              inputs:
                p2b: 220
            - schema: s2

        tasks:
          - schema: s0
            inputs:
              p1: 100
          - schema: system_analysis
          - schema: s3
        """
    )
    wk = hf.Workflow.from_YAML_string(wk_yaml, path=tmp_path)

    # basic check of param dependendices
    s0_di = wk.tasks.s0.elements[0].get_data_idx()
    s1_di = wk.tasks.s1.elements[0].get_data_idx()
    s2_di = wk.tasks.s2.elements[0].get_data_idx()
    s3_di = wk.tasks.s3.elements[0].get_data_idx()

    assert s0_di["outputs.p2"] == s1_di["inputs.p2"]
    assert s1_di["outputs.p3"] == s2_di["inputs.p3"]
    assert s2_di["outputs.p4"] == s3_di["inputs.p4"]


def test_basic_meta_task_workflow_API(new_null_config, tmp_path):
    """as above but using Python API."""
    # normal task schemas:
    s0 = hf.TaskSchema(
        objective="s0",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command='echo "$((<<parameter:p1>> + 1))"',
                        stdout="<<int(parameter:p2)>>",
                    )
                ]
            )
        ],
    )
    s1 = hf.TaskSchema(
        objective="s1",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command='echo "$((<<parameter:p2>> + 1))"',
                        stdout="<<int(parameter:p3)>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="s2",
        inputs=[hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command='echo "$((<<parameter:p3>> + 1))"',
                        stdout="<<int(parameter:p4)>>",
                    )
                ]
            )
        ],
    )
    s3 = hf.TaskSchema(
        objective="s3",
        inputs=[hf.SchemaInput("p4")],
        outputs=[hf.SchemaOutput("p5")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command='echo "$((<<parameter:p4>> + 1))"',
                        stdout="<<int(parameter:p5)>>",
                    )
                ]
            )
        ],
    )

    # meta=task schema:
    ms = hf.MetaTaskSchema(
        objective="system_analysis",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p4")],
    )

    # meta-task:
    m1 = hf.MetaTask(
        schema=ms,
        tasks=[
            hf.Task(schema=s1),
            hf.Task(schema=s2),
        ],
    )

    # workflow template tasks list:
    tasks = [
        hf.Task(schema=s0, inputs={"p1": 100}),
        m1,
        hf.Task(schema=s3),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="meta_task_workflow",
        tasks=tasks,
        path=tmp_path,
    )

    # basic check of param dependendices
    s0_di = wk.tasks.s0.elements[0].get_data_idx()
    s1_di = wk.tasks.s1.elements[0].get_data_idx()
    s2_di = wk.tasks.s2.elements[0].get_data_idx()
    s3_di = wk.tasks.s3.elements[0].get_data_idx()

    assert s0_di["outputs.p2"] == s1_di["inputs.p2"]
    assert s1_di["outputs.p3"] == s2_di["inputs.p3"]
    assert s2_di["outputs.p4"] == s3_di["inputs.p4"]


def test_meta_task_custom_parametrisation(new_null_config, tmp_path):
    """test customising the parametrisation of inputs, sequences, and resources within the
    `tasks` list."""
    wk_yaml = dedent(
        """\
        name: test_metatask_multi_element_sets_custom_parametrisation
        template_components:
          task_schemas:
            - objective: s1
              inputs: 
                - parameter: p1
                - parameter: p2
              outputs:
                - parameter: p3
              actions:
                - commands:
                  - command: echo "$((<<parameter:p1>> + <<parameter:p2>>))"
                    stdout: <<int(parameter:p3)>>

          meta_task_schemas:
            - objective: system_analysis
              inputs:
                - parameter: p1
                - parameter: p2
              outputs:
                - parameter: p3

        meta_tasks:
          system_analysis:
            - schema: s1
              element_sets:
                - inputs:
                    p1: 100
                    p2: 200
                - inputs:
                    p1: 100
                  sequences:
                    - path: inputs.p2
                      values: [200, 201]
        tasks:
          - schema: system_analysis
            inputs:
              s1: # should apply to first element set by default
                p1: 101
            resources:
              s1: # should apply to first element set by default
                any:
                  num_cores: 2
          - schema: system_analysis
            inputs:
              s1.0: # applies to first element set of s1
                p1: 102
              s1.1: # applies to second element set of s1
                p1: 103
            sequences:
              s1.1: # sequences list in second element set is replaced with this list:
                - path: inputs.p2
                  values: [300, 301]
        """
    )
    wk = hf.Workflow.from_YAML_string(wk_yaml, path=tmp_path)

    assert wk.tasks.s1_1.template.element_sets[0].resources[0].num_cores == 2  # modified
    assert (
        wk.tasks.s1_2.template.element_sets[0].resources[0].num_cores is None
    )  # unaffected

    assert wk.tasks.s1_1.template.element_sets[0].inputs[0].value == 101  # modified
    assert wk.tasks.s1_1.template.element_sets[0].inputs[1].value == 200  # unaffected
    assert wk.tasks.s1_1.template.element_sets[1].sequences[0].values == [
        200,
        201,
    ]  # unaffected

    assert wk.tasks.s1_2.template.element_sets[0].inputs[0].value == 102  # modified
    assert wk.tasks.s1_2.template.element_sets[1].inputs[0].value == 103  # modified
    assert wk.tasks.s1_2.template.element_sets[1].sequences[0].values == [
        300,
        301,
    ]  # modified


def test_meta_task_custom_parametrisation_raises_on_bad_schema_name(
    new_null_config, tmp_path
):
    wk_yaml = dedent(
        """\
      name: test_metatask_raise_on_bad_schema_name
      template_components:
        task_schemas:
          - objective: s1
            inputs: 
              - parameter: p1
              - parameter: p2
            outputs:
              - parameter: p3
            actions:
              - commands:
                - command: echo "$((<<parameter:p1>> + <<parameter:p2>>))"
                  stdout: <<int(parameter:p3)>>

        meta_task_schemas:
          - objective: system_analysis
            inputs:
              - parameter: p1
              - parameter: p2
            outputs:
              - parameter: p3

      meta_tasks:
        system_analysis:
          - schema: s1
            element_sets:
              - inputs:
                  p1: 100
                  p2: 200
              - inputs:
                  p1: 100
                sequences:
                  - path: inputs.p2
                    values: [200, 201]
      tasks:
        - schema: system_analysis
          resources:
            BAD_SCHEMA_NAME: # should raise!
              any:
                num_cores: 2
      """
    )
    with pytest.raises(UnknownMetaTaskConstitutiveSchema):
        wk = hf.Workflow.from_YAML_string(wk_yaml, path=tmp_path)
