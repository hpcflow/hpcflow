"""Check test configuration with and without optional pytest plugins."""

from types import SimpleNamespace

import pytest

from hpcflow.app import app as hf
from hpcflow.tests import conftest


@pytest.mark.parametrize("with_xdist", [False, True])
def test_configure_node_hook_is_optional(with_xdist: bool) -> None:
    manager = pytest.PytestPluginManager()
    if with_xdist:
        manager.add_hookspecs(pytest.importorskip("xdist.newhooks"))
    manager.register(conftest)

    # Executables do not load xdist, so its hook specification is absent.
    manager.check_pending()

    if with_xdist:
        node = SimpleNamespace(workerinput={})
        manager.hook.pytest_configure_node(node=node)
        overrides = node.workerinput["hpcflow_config_overrides"]
        assert overrides == hf.config._overrides
        assert overrides is not hf.config._overrides
