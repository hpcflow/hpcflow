"""Preserve lazy app binding and derived-app overrides after mixin extraction."""

import inspect

import pytest

import hpcflow.app as app_module
from hpcflow.app import app as hf
from hpcflow.sdk.app import BaseApp


@pytest.mark.parametrize(
    "name", ["show", "show_legend", "get_known_submissions", "unregister"]
)
def test_inherited_monitoring_callable_remains_cached_and_exported(name):
    method = getattr(hf, name)
    assert method is getattr(hf, name)
    assert method is getattr(app_module, name)
    assert method.__module__ == hf.module
    assert inspect.signature(method) == inspect.signature(getattr(hf, f"_{name}"))


@pytest.mark.parametrize(
    "name", ["show", "show_legend", "get_known_submissions", "unregister"]
)
def test_derived_app_monitoring_overrides_use_host_binding(name, monkeypatch):
    calls = []

    class DerivedApp(BaseApp):
        pass

    def implementation(self, *args, **kwargs):
        """Monitoring override for {app_name}."""
        calls.append((self, args, kwargs))
        return "derived result"

    monkeypatch.setattr(DerivedApp, "_instances", {})
    monkeypatch.setattr(DerivedApp, f"_{name}", implementation)
    app = DerivedApp(
        name="Monitoring test app",
        version="0",
        module="monitoring_test_app",
        description="Test inherited monitoring binding",
        gh_org="hpcflow",
        gh_repo="hpcflow",
        config_options=hf.config_options,
        scripts_dir="hpcflow.data.scripts",
    )
    method = getattr(app, name)
    assert method() == "derived result"
    assert calls == [(app, (), {})]
    assert method is getattr(app, name)
    assert method.__module__ == "monitoring_test_app"
    assert method.__doc__ == "Monitoring override for Monitoring test app."
