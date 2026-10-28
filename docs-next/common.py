"""Shared configuration for the isolated documentation prototypes."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def configure(name, namespace):
    namespace.update(
        project=name,
        author=f"{name} contributors",
        copyright=f"2026, {name} contributors",
        root_doc="index",
        extensions=["sphinx_design", "sphinx_copybutton"],
        html_theme="furo",
        html_title=f"{name} documentation",
        html_static_path=[str(ROOT / "_static")],
        html_css_files=["prototype.css"],
        templates_path=[str(ROOT / "_templates")],
        exclude_patterns=[],
        html_theme_options={
            "announcement": "Documentation preview",
            "light_css_variables": {
                "color-brand-primary": "#17655c" if name == "hpcflow" else "#7044a0",
                "color-brand-content": "#17655c" if name == "hpcflow" else "#7044a0",
            },
            "dark_css_variables": {
                "color-brand-primary": "#82d9ca" if name == "hpcflow" else "#d6b2ff",
                "color-brand-content": "#82d9ca" if name == "hpcflow" else "#d6b2ff",
            },
        },
        html_context={"sections": [
            ("install/index", "Install"), ("user-guide/index", "User guide"),
            ("workflows/index", "Workflows"), ("reference/index", "Reference"),
            ("development/index", "Development"),
        ]},
        rst_prolog=f".. |app| replace:: {name}\n",
        nitpicky=True,
    )
