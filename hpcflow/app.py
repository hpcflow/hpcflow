from __future__ import annotations
from hpcflow import __version__, _app_name
from hpcflow.sdk import app as sdk_app
from hpcflow.sdk.config import ConfigOptions


# provide access to app attributes:
__getattr__ = sdk_app.get_app_attribute

# ensure docs/help can see dynamically loaded attributes:
__all__ = sdk_app.get_app_module_all()
__dir__ = sdk_app.get_app_module_dir()

# set app-level config options:
config_options = ConfigOptions(
    directory_env_var="HPCFLOW_CONFIG_DIR",
    default_directory="~/.hpcflow-new",
)

# load built in template components (in this case, for demonstration purposes):
template_components = sdk_app.BaseApp.load_builtin_template_component_data(
    "hpcflow.data.template_components"
)

# initialise the App object:
app: sdk_app.BaseApp = sdk_app.BaseApp(
    name=_app_name,
    version=__version__,
    module=__name__,
    docs_import_conv="hf",
    description="Computational workflow management",
    gh_org="hpcflow",
    gh_repo="hpcflow-new",
    config_options=config_options,
    template_components=template_components,
    scripts_dir="data.scripts",  # relative to root package
    workflows_dir="data.workflows",  # relative to root package
    demo_data_dir="hpcflow.data.demo_data",
    demo_data_manifest_dir="hpcflow.data.demo_data_manifest",
    docs_url="https://hpcflow.github.io/docs/stable",
)  #: |app|
