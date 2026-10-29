# Documentation prototype

This directory contains three independent Sphinx/Furo sites: `hpcflow/`,
`matflow/`, and `matflow-landing/`. Furo is the theme used by the pipx site
inspected for this prototype: <https://pipx.pypa.io/stable/index.html>.
The five-section horizontal navigation is a small addition to Furo. Selecting a
section opens its overview and shows only that section's pages in the left sidebar.
Nested pages keep the same section active, with expandable groups and the current
page highlighted. Home and search pages show a short site menu instead. The
sidebar is generated from the existing Sphinx toctrees by `section_navigation.py`;
adding a page to a section's toctree also adds it to that section's sidebar.
Light/dark mode, mobile navigation, search and copy buttons use the theme and
Sphinx extensions. No application import or notebook execution is needed.

Both projects' existing `docs/` directories are untouched. All prototype files
live here for review; MatFlow does not need to be modified to preview its site.

## Build and view

From the hpcflow repository root, using Python 3.11 or newer:

```console
python -m venv docs-next/.venv
```

Activate that environment (`docs-next/.venv/Scripts/Activate.ps1` on PowerShell,
or `source docs-next/.venv/bin/activate` on Linux/macOS), then run:

```console
python -m pip install -r docs-next/requirements.txt
python docs-next/build.py
python -m http.server 8000 --bind 127.0.0.1 --directory docs-next/_build
```

Open <http://127.0.0.1:8000/> and choose a site. Serving the common output root
keeps the MatFlow landing page's links to its docs working. Every build treats
Sphinx warnings as errors. The output can also be opened as local HTML, though
search works best through the local server.

## Refresh examples

The gallery contains five curated workflows copied from the local checkouts,
including uncommitted edits. `source-manifest.json` records the revision,
local-change status and workflow hashes. These are documentation snapshots,
not claims that every example has been executed successfully.

With hpcflow and MatFlow checked out side by side:

```console
python docs-next/build.py --refresh
```

For another layout, pass `--hpcflow-repo PATH --matflow-repo PATH`.
Refresh reads the sibling checkout but writes only inside this directory.
Normal builds and CI use the committed snapshots and do not require MatFlow,
network access to source repositories or scientific software installations.

## Pull-request previews

`.github/workflows/docs-next.yml` builds all sites on pull requests and uploads
`documentation-prototype` as an Actions artifact. Download and extract it, then
serve its root with `python -m http.server 8000 --bind 127.0.0.1`.

This is a downloadable preview, not a hosted PR URL. For hosted previews, configure
a dedicated GitHub Pages deployment branch or another static host before adding
deployment. Do not point experimental previews at the production docs domain.
The prototype workflow has read-only repository permissions and does not publish.
Hosting configuration and approval are still needed before that prerequisite in
the supplied notes is fully met.

## Content and migration decisions

The five navigation sections follow the supplied notes. hpcflow explains the
engine; MatFlow adds scientific workflows. The separate landing site is a
starting point for matflow.io, with installation, community, project and citation
links. Proposed services and runtime features are explicitly labelled as future
work rather than presented as working features.

The feature pages are introductions. Full API/CLI signatures, configuration
schema trees and parameter constructors link to the current documentation until
their generators are migrated. Solver-specific tutorials use real YAML snapshots
but still need execution checks and expanded scientific explanations. Gallery
themes provide initial grouping; interactive tags and the complete workflow
catalogue remain future work.

| Existing content | Prototype destination | Status |
| --- | --- | --- |
| Installation and alternative installs | `install/` | Rewritten introduction |
| Getting started | `user-guide/` | New short reader journey |
| How-to guides | `user-guide/features/` | Concept introductions; detailed syntax still linked |
| Demo workflows | `workflows/` | Five curated source snapshots across both projects |
| Template components | `reference/catalogue.rst` | Counts and source links; generated detail pending |
| API, CLI, configuration schema | `reference/` | Existing generated references linked |
| Contributing and development | `development/` | New overview and migration roadmap |

Before cutover: migrate generated references, verify examples and solver versions,
review citations and case studies, agree on shared content ownership, add redirects
and version selection, and configure hosted PR previews. Search each site from its
own root; there is no combined search index. The plain standalone landing page
currently shares the docs theme, so its final marketing design can be reviewed
separately.
