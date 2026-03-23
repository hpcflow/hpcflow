import os
from pathlib import Path

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import SecretNotFoundError
from hpcflow.sdk.core.warnings import SecretExposedWarning


def test_set_get_delete_secret():
    key, value = "SECRET_KEY", "SECRET_VALUE"
    hf.set_secret(key, value)
    assert hf.get_secret(key) == value
    hf.delete_secret(key)
    with pytest.raises(SecretNotFoundError):
        hf.get_secret(key)


@pytest.mark.integration
@pytest.mark.parametrize("combine_scripts", [False, True])
def test_secret_in_env(tmp_path: Path, reload_template_components, combine_scripts: bool):
    key, value = "TOP_SECRET_KEY", "TOP_SECRET_VALUE"
    hf.set_secret(key, value)

    try:

        env = hf.Environment(name="test_env", secrets=[key])
        hf.envs.add_object(env, skip_duplicates=True)

        cmd = f"echo $env:{key}" if os.name == "nt" else f"echo ${key}"
        act = hf.Action(
            commands=[hf.Command(command=cmd, stdout="<<parameter:p2>>")],
            environments=[hf.ActionEnvironment(environment="test_env")],
        )
        s1 = hf.TaskSchema(
            objective="hello",
            actions=[act],
            inputs=[],
            outputs=[hf.Parameter("p2")],
        )
        tasks = [hf.Task(s1)]
        wk = hf.Workflow.from_template_data(
            template_name="test_secrets",
            tasks=tasks,
            path=tmp_path,
            config={"log_file_level": "debug"},
            resources={
                "any": {"combine_scripts": combine_scripts, "write_app_logs": True}
            },
            store="zarr",
        )
        wk.submit(wait=True, status=False, add_to_known=False)

        # test secret not exposed in debug logs:
        run = wk.get_EARs_from_IDs([0])[0]
        log_path = run.get_app_log_path()
        assert log_path.is_file()
        log_txt = log_path.read_text()
        assert key in log_txt
        assert value not in log_txt

        # test secret not exposed in workflow metadata (Zarr only)
        metadata = Path(wk.path, ".zattrs").read_text()
        assert key in metadata
        assert value not in metadata

        # test secret value:
        p2 = wk.tasks[0].elements[0].outputs.p2
        assert isinstance(p2, hf.ElementParameter)
        assert p2.value == value

    finally:
        hf.delete_secret(key)


def test_secret_exposed_warning():
    with pytest.warns(SecretExposedWarning):
        hf.Environment(name="test_env", setup="export password=1234")

    with pytest.warns(SecretExposedWarning):
        hf.Environment(name="test_env", setup="$env:password = '1234'")

    with pytest.warns(SecretExposedWarning):
        hf.Environment(name="test_env", setup="PASSword=1234")

    with pytest.warns(SecretExposedWarning):
        hf.Environment(name="test_env", setup="$env:PASSword = '1234'")

    with pytest.warns(SecretExposedWarning):
        hf.Environment(
            name="test_env",
            executables=[
                hf.Executable(
                    label="my_exec",
                    instances=[
                        hf.ExecutableInstance(
                            parallel_mode=None,
                            num_cores=1,
                            command="export password=1234",
                        )
                    ],
                )
            ],
        )
