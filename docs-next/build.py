"""Build all prototype sites; optionally refresh curated source snapshots."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
WORKFLOWS = {
    "hpcflow": [
        ("workflow_1", "A first local workflow", "Getting started",
         "A small, OS-agnostic task using a built-in test schema. Use this to check basic submission before integrating another tool."),
    ],
    "matflow": [
        ("generate_volume_element_from_statistics", "Generate a volume element", "Microstructure",
         "Create a Dream.3D pipeline from phase statistics and generate a volume element. This template also illustrates texture inputs. It requires the environments for Dream.3D and the selected visualisation tasks."),
        ("sample_texture_random", "Sample a random texture", "Microstructure",
         "Start from the shipped random-texture example and inspect its orientation inputs and task schema requirements before running it."),
        ("tension_DAMASK_Al", "Crystal plasticity with DAMASK", "Crystal plasticity",
         "Inspect an aluminium loading example built from DAMASK-related task schemas. Configure the matching DAMASK and supporting Python environments before submission."),
        ("subset_simulation_toy_model", "Subset simulation: toy model", "Uncertainty quantification",
         "Explore the structure of a subset-simulation workflow with a toy model before moving to expensive solver-based uncertainty quantification."),
    ],
}


def page(title, text):
    return f"{title}\n{'=' * len(title)}\n\n{text.strip()}\n"


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def refresh(repos):
    from ruamel.yaml import YAML

    # Validate every requested input before updating any snapshots.
    sources = []
    yaml = YAML(typ="safe")
    for app, repo in repos.items():
        for stem, *_ in WORKFLOWS[app]:
            source = repo / app / "data/workflows" / f"{stem}.yaml"
            data = yaml.load(source.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or not data.get("tasks"):
                raise ValueError(f"No tasks in {source}")
            sources.append(source)
        for component in ("parameters", "task_schemas", "environments", "command_files"):
            source = repo / app / "data/template_components" / f"{component}.yaml"
            if not source.is_file():
                raise FileNotFoundError(source)
    manifest = {}
    for app, repo in repos.items():
        commit = subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "-C", str(repo), "status", "--porcelain", "--", f"{app}/data"],
            text=True,
        ).strip()
        manifest[app] = {"commit": commit, "data_has_local_changes": bool(status), "sha256": {}}
        links = []
        for stem, title, tag, description in WORKFLOWS[app]:
            source = repo / app / "data/workflows" / f"{stem}.yaml"
            raw = source.read_bytes()
            manifest[app]["sha256"][str(source.relative_to(repo)).replace("\\", "/")] = hashlib.sha256(raw).hexdigest()
            dest = ROOT / app / "examples" / source.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, dest)
            source_url = f"https://github.com/hpcflow/{app}/blob/{commit}/{app}/data/workflows/{source.name}"
            write(ROOT / app / "workflows" / f"{stem}.rst", page(title, f'''
**Theme:** {tag}

{description}

Run the example
---------------

Check the environments required by each schema in the YAML. Confirm the installed
version lists this demo before using its name:

.. code-block:: console

   {app} demo-workflow --list
   {app} demo-workflow copy {source.name} .
   {app} go {source.name}
   {app} show -f

The gallery is a snapshot of the local source tree, so a released package may have
different demos or schema requirements. A source snapshot is not an execution test.
Review input paths and workload size before submitting it.

Inspect the template
--------------------

:download:`Download YAML <../examples/{source.name}>` · `Upstream source <{source_url}>`_

The downloadable file preserves the local checkout, including any uncommitted edits;
the upstream link points to the recorded commit.

.. literalinclude:: ../examples/{source.name}
   :language: yaml

Continue with :doc:`../user-guide/workflow-structure` to locate results.
'''))
            links.append(f"   {stem}")
        themes = {}
        for stem, title, tag, _ in WORKFLOWS[app]:
            themes.setdefault(tag, []).append(f"* :doc:`{title} <{stem}>`")
        grouped = "\n\n".join(f"{tag}\n{'-' * len(tag)}\n\n" + "\n".join(items) for tag, items in themes.items())
        write(ROOT / app / "workflows/index.rst", page("Workflows", f'''
Start from a real template. These examples are grouped by scientific theme;
each page includes the YAML, a download and a source link.

The examples are curated from the checked-out repository. Use
``{app} demo-workflow --list`` for the complete list in your installed version.

{grouped}

.. toctree::
   :hidden:

{chr(10).join(links)}
'''))
        components = []
        for component, label in [("parameters", "Parameters"), ("task_schemas", "Task schemas"), ("environments", "Environments"), ("command_files", "Command files")]:
            source = repo / app / "data/template_components" / f"{component}.yaml"
            entries = yaml.load(source.read_text(encoding="utf-8"))
            components.append(f"* **{label}**: {len(entries)} source definitions. "
                              f"`Browse {label.lower()} YAML <https://github.com/hpcflow/{app}/blob/{commit}/{app}/data/template_components/{component}.yaml>`_")
        write(ROOT / app / "reference/catalogue.rst", page("Built-in component sources", f'''
Component definitions are the source of truth for the schemas, executable labels
and parameter types used by a template. These counts include internal definitions;
they are not a count of supported public workflows.

{chr(10).join(components)}

Snapshot revision: ``{commit}``.
Counts reflect the local checkout when refreshed, including local changes.
The source links show the recorded upstream commit. See ``source-manifest.json``
in the prototype root for workflow file hashes and local-change indicators.

Scripts live in the `application scripts directory <https://github.com/hpcflow/{app}/tree/{commit}/{app}/data/scripts>`_.
For generated parameter constructors and input/output relationships, follow the
current reference links in :doc:`index` until those pages are migrated.
'''))
    write(ROOT / "source-manifest.json", json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="Refresh gallery and component metadata from both checkouts")
    parser.add_argument("--hpcflow-repo", type=Path, default=ROOT.parent)
    parser.add_argument("--matflow-repo", type=Path, default=ROOT.parent.parent / "matflow")
    args = parser.parse_args()
    if args.refresh:
        refresh({"hpcflow": args.hpcflow_repo.resolve(), "matflow": args.matflow_repo.resolve()})
    for site in ("hpcflow", "matflow", "matflow-landing"):
        subprocess.run([
            sys.executable, "-m", "sphinx", "-b", "html", "-n", "-W", "--keep-going",
            str(ROOT / site), str(ROOT / "_build" / site),
        ], check=True)
    write(ROOT / "_build/index.html", '''<!doctype html>
<html lang="en"><meta charset="utf-8"><title>Documentation prototypes</title>
<main><h1>Documentation prototypes</h1><ul>
<li><a href="hpcflow/index.html">hpcflow documentation</a></li>
<li><a href="matflow/index.html">MatFlow documentation</a></li>
<li><a href="matflow-landing/index.html">MatFlow landing page</a></li>
</ul></main></html>''')
    from check_links import check

    check(ROOT / "_build")
    print(f"Preview: {ROOT / '_build/index.html'}")


if __name__ == "__main__":
    main()
