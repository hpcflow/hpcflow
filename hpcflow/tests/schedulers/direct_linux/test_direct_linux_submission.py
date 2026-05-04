from importlib import resources
import pytest
from hpcflow.app import app as hf


@pytest.mark.direct_linux
def test_workflow_1(tmp_path):
    act = hf.Action(commands=[hf.Command("echo 'Buenas!'")])
    ts = hf.TaskSchema(objective="hello", actions=[act])
    wkt = hf.WorkflowTemplate(name="greetings", tasks=[hf.Task(schema=ts)])
    wf = hf.Workflow.from_template(name="saludos", template=wkt)
    wf.submit(wait=True, add_to_known=False)
