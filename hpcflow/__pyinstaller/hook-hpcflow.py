from PyInstaller.utils.hooks import collect_data_files

from hpcflow.sdk import sdk_classes


# most of the modules in `sdk_classes` are imported on-demand via the app object:
hiddenimports = [
    *sdk_classes.values(),
    "hpcflow.sdk.data",
    "hpcflow.data.demo_data_manifest",
    "hpcflow.data.scripts",
    "hpcflow.data.template_components",
    "hpcflow.data.workflows",
    "hpcflow.tests.data",
    "hpcflow.sdk.core.test_utils",
    "hpcflow.sdk.utils.patches",
    "click.testing",
    "requests",  # for GitHub fsspec file system
    "fsspec.implementations.github",  # for GitHub fsspec file system
]

datas = (
    collect_data_files("hpcflow.sdk.data")
    + collect_data_files("hpcflow.data.demo_data_manifest")
    + collect_data_files(
        "hpcflow.data.scripts", include_py_files=True, excludes=("**/__pycache__",)
    )
    + collect_data_files("hpcflow.data.template_components")
    + collect_data_files("hpcflow.data.workflows")
    + collect_data_files(
        "hpcflow.tests", include_py_files=True, excludes=("**/__pycache__",)
    )
    + collect_data_files("hpcflow.tests.data")
)
