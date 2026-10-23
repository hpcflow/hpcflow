These GitHub actions workflows are generated from Jinja templates from https://github.com/hpcflow/python-release-workflow.

The `generate_workflows.py` file is invoked like this to generate all of the workflow YAML file:

```yaml
cd .github/workflows
python generate_workflows.py . vars.jsonc
```

## Recovering an unpublished stable release

The manually maintained `release-recovery.yml.in` template and its generated
`release-recovery.yml` caller use the shared release workflow's existing-tag mode.
Merge the shared `github-support` recovery implementation into its `main` branch
before merging this caller into hpcflow's default branch and `main`.

In GitHub Actions, select **Recover stable release**, select the **main** branch,
and enter the existing release tag and previous stable tag. For the interrupted
0.3.0 release these are `v0.3.0` and `v0.2.0`. Review the tags and check
**confirm-publication** only when ready to publish to GitHub, PyPI, and the
documentation website. Dispatching without confirmation or from another branch
fails explicitly. This workflow shares the normal release concurrency group.

Recovery checks that the version has not been published, rebuilds from the
existing release commit, and skips version/tag changes and pushes to the hpcflow
repository. It still publishes packages, binaries, and the documentation website.
Do not use it for a version already published to either GitHub or PyPI; that
requires inspecting and completing the remaining publication steps separately.
