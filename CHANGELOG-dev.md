
<a name="v0.2.0a194"></a>
## [v0.2.0a194](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a193...v0.2.0a194) - 2025.03.05


<a name="v0.2.0a193"></a>
## [v0.2.0a193](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a192...v0.2.0a193) - 2025.02.19

### 🐛 Bug Fixes

* fix version of zarr


<a name="v0.2.0a192"></a>
## [v0.2.0a192](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a191...v0.2.0a192) - 2025.02.19


<a name="v0.2.0a191"></a>
## [v0.2.0a191](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a190...v0.2.0a191) - 2025.02.03

### 🐛 Bug Fixes

* correct logic for sge_parallel_env with 1 core

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a190"></a>
## [v0.2.0a190](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a189...v0.2.0a190) - 2024.12.13

### 🐛 Bug Fixes

* replace call to non-existent method

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a189"></a>
## [v0.2.0a189](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a188...v0.2.0a189) - 2024.12.12

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a188"></a>
## [v0.2.0a188](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a187...v0.2.0a188) - 2024.12.12

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a187"></a>
## [v0.2.0a187](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a186...v0.2.0a187) - 2024.12.08

### ♻ Code Refactoring

* make some util funcs reusable in MatFlow

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a186"></a>
## [v0.2.0a186](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a185...v0.2.0a186) - 2024.12.06

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a185"></a>
## [v0.2.0a185](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a184...v0.2.0a185) - 2024.12.05

### 🐛 Bug Fixes

* missing GHA updates!

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a184"></a>
## [v0.2.0a184](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a183...v0.2.0a184) - 2024.12.05

### 🐛 Bug Fixes

* fix release workflow

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a183"></a>
## [v0.2.0a183](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a182...v0.2.0a183) - 2024.12.05

### 🐛 Bug Fixes

* try to fix compression of centos pyinstaller directory build


<a name="v0.2.0a182"></a>
## [v0.2.0a182](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a181...v0.2.0a182) - 2024.12.04

### 🐛 Bug Fixes

* sync again with hpcflow-shared-docs
* sync with hpcflow-shared-docs
* sync with python-release-workflow repo
* release workflow pyinstaller build on CentOS
* pyinstaller build on CentOS
* fix pytest call in centos integration tests
* fix pytest call in centos integration tests
* run centOS integration tests in a docker run step rather than using a job `container`
* run centOS unit tests in a docker run step rather than using a job `container`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a181"></a>
## [v0.2.0a181](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a180...v0.2.0a181) - 2024.11.14

### 🐛 Bug Fixes

* bump ruamel.yaml

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a180"></a>
## [v0.2.0a180](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a179...v0.2.0a180) - 2024.09.24

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a179"></a>
## [v0.2.0a179](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a178...v0.2.0a179) - 2024.09.21

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a178"></a>
## [v0.2.0a178](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a177...v0.2.0a178) - 2024.07.14

### ✨ Features

* exclude rechunk backups from zip by default, add option to include execute
* add rechunk CLI
* add `Workflow.rechunk` for zarr store type to reduce number of files

### 🐛 Bug Fixes

* update GHA workflows
* centos GHAs - build exes
* centos GHAs

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a177"></a>
## [v0.2.0a177](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a176...v0.2.0a177) - 2024.05.22

### ♻ Code Refactoring

* add `LoopCache` and use in `WorkflowLoop.add_iteration`
* add a cache for later use in `WorkflowLoop.add_iteration`

### ⚡ Performance Improvements

* add a dependencies cache to pre-compute dependencies during loop preparation
* batch up element retrieval in `get_elements_from_IDs`
* batch up `PersistentStore.update_param_source`
* add `num_EARS` to persistentstore cache
* batch up element retrieval in `Workflow.get_element_iterations_from_IDs`
* defer `initialise_EARs` in `add_iteration` and support passing iter_IDs to `initialise_EARs`

### ✨ Features

* print loop name in status when adding a loop

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a176"></a>
## [v0.2.0a176](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a175...v0.2.0a176) - 2024.05.15

### 🐛 Bug Fixes

* remove debugging prints
* sourcing iterated inputs from the latest parent loop iteration

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a175"></a>
## [v0.2.0a175](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a174...v0.2.0a175) - 2024.05.13

### 🐛 Bug Fixes

* restrict Zarr version to workaround [#680](https://github.com/hpcflow/hpcflow-new/issues/680)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a174"></a>
## [v0.2.0a174](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a173...v0.2.0a174) - 2024.05.13

### 🐛 Bug Fixes

* multi-level nested loops
* nested loops, and raise on some scenarios we cannot yet support
* raise on missing element group
* iteration loop_idx for nested loops

### 👷 Build changes

* update platformdirs
* update binary download links file [skip ci]


<a name="v0.2.0a173"></a>
## [v0.2.0a173](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a172...v0.2.0a173) - 2024.05.02

### 🐛 Bug Fixes

* fix failing MacOS GHA test by marking as expected to fail


<a name="v0.2.0a172"></a>
## [v0.2.0a172](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a171...v0.2.0a172) - 2024.05.02

### 🐛 Bug Fixes

* missing endraw again


<a name="v0.2.0a171"></a>
## [v0.2.0a171](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a170...v0.2.0a171) - 2024.05.02

### 🐛 Bug Fixes

* missing endraw
* missing arg in `rate_limit_safe_url_to_fs`
* apply pytest args option to build-exe workflow tests
* apply pytest args option to all tests
* allow passing pytest args in GHA unit tests
* try to use GITHUB_TOKEN to auth in `url_to_fs` when running tests
* add a retry loop on calling `fsspec`s `url_to_fs` function in demo-data-related methods

### 👷 Build changes

* sync GHA updates


<a name="v0.2.0a170"></a>
## [v0.2.0a170](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a169...v0.2.0a170) - 2024.05.02

### ✨ Features

* add `input_sources` to `ElementPropagation`

### 🐛 Bug Fixes

* consider non-local inputs as `task_source_type="input"` sources; fix [#671](https://github.com/hpcflow/hpcflow-new/issues/671)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a169"></a>
## [v0.2.0a169](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a168...v0.2.0a169) - 2024.05.01

### 🐛 Bug Fixes

* make loop termination command check conditional on any loops existing in the workflow

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a168"></a>
## [v0.2.0a168](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a167...v0.2.0a168) - 2024.04.28

### 🐛 Bug Fixes

* add a download-artifact[@v3](https://github.com/v3) step in release-github-PyPI to grab CentOS (docker) built executable which cannot use upload-artifact[@v4](https://github.com/v4)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a167"></a>
## [v0.2.0a167](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a166...v0.2.0a167) - 2024.04.28

### 🐛 Bug Fixes

* revert upload/download-artifact updates in CentOS (docker) job in build-exes
* revert upload/download-artifact updates in CentOS (docker) job in release

### 👷 Build changes

* update GHAs to fix deprecation warnings
* update binary download links file [skip ci]


<a name="v0.2.0a166"></a>
## [v0.2.0a166](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a165...v0.2.0a166) - 2024.04.28

### ♻ Code Refactoring

* remove unused method
* remove unused imports in task.py

### ✨ Features

* maintain input source `element_iter` order if `allow_coincident_task_input_sources` is `False`
* support user-specified `element_iters` in `InputSource` with `source_type="task"`

### 🐛 Bug Fixes

* partially revert GHA version updates to fix running in old-GLIBC container
* fix macos runner to macos-13 to workaround macos-latest issues in [#657](https://github.com/hpcflow/hpcflow-new/issues/657)
* validate `nesting_order` paths
* support sourcing inputs from sub-parameter sequences in multiple element sets in the upstream task
* raise on no coincident input sources from the same task
* add `allow_non_coincident_task_sources` to `Task`
* raise on unavailable user-specified input source
* for sub-parameters that appear in multiple element sets of the same ask, fix `Task.provides_parameters`

### 👷 Build changes

* update GHAs to fix deprecation warnings
* update GHAs to fix deprecation warnings
* update GHAs to fix deprecation warnings


<a name="v0.2.0a165"></a>
## [v0.2.0a165](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a164...v0.2.0a165) - 2024.04.25

### ✨ Features

* add `seed` input when creating random a `ValueSequence`
* create a `ValueSequence` from a random sample of a uniform distribution a

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a164"></a>
## [v0.2.0a164](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a163...v0.2.0a164) - 2024.04.23

### ✨ Features

* support rules in `Command`

### 🐛 Bug Fixes

* get_info_html when multiple envs are present

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a163"></a>
## [v0.2.0a163](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a162...v0.2.0a163) - 2024.04.19

### ♻ Code Refactoring

* rename `ElementSet.envrionment` to `env_preset` to disambiguate from `environments`
* simplify `BaseApp._load_script`, allow arbitrary script/dir naming

### ✨ Features

* add `script_pass_env_spec` to Action
* support template-level `envrionments` and `env_presets` and add `template_components` scope to `WorkflowTemplate._from_data` for inline task schemas etc
* support defining environment presets in `TaskSchema`
* support `<<env:[SPECIFIER]>>` variables in action script paths
* use specifiers to distinguish multiple environments of the same name
* return workflow object from `make_and_submit_(demo_)workflow`

### 🐛 Bug Fixes

* use workflow template validation schema
* Task._merge_envs for sequences on `env_preset`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a162"></a>
## [v0.2.0a162](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a161...v0.2.0a162) - 2024.04.09

### 🐛 Bug Fixes

* func `ElementResources.get_jobscript_hash` incorrect hashing of `scheduler_args["options"]`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a161"></a>
## [v0.2.0a161](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a160...v0.2.0a161) - 2024.04.09

### 🐛 Bug Fixes

* consider not-yet-set submission start/end times in `_get_known_submissions`
* cache submission start and end times in known-submissions file
* add missing path arg in a test
* properly catch `KeyboardInterrupt` in `show` command
* treat `InputFileGenerator` and `OutputFileParser` script snippets similarly to standard actions scripts

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a160"></a>
## [v0.2.0a160](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a159...v0.2.0a160) - 2024.04.05

### ✨ Features

* add live status to make command
* add more Rich status to `show` command
* implement a `PersistentStore` cache and use during submission
* add `--status` submission option to (not) show the Rich live status during submission
* add `--cancel` submission option to help with testing/benchmarking

### 🐛 Bug Fixes

* increase sleep time in main script tests to decrease chance of GHA ubuntu failures
* try to temporarily fix occassional GHA failure on Ubuntu with recent Pythons
* try to temporarily fix occassional GHA failure on Ubuntu with recent Pythons
* batch up `commit_EAR_submission_indices`
* optimise `Submission.get_{start,end}_time`
* use persistent store cache in `_get_known_submissions`
* add more `TimeIt` decorators
* add more `TimeIt` decorators
* add more `TimeIt` decorators
* missing import
* missing association of new class to App object
* streamline run dir snapshot data stored in EAR data
* add `PersistentStore.param_sources_cache` and `parameter_cache`
* optimise `ElementIteration.get_parameter_sources` to call `Workflow.get_parameter_sources` once

### 👷 Build changes

* merge develop
* update binary download links file [skip ci]


<a name="v0.2.0a159"></a>
## [v0.2.0a159](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a158...v0.2.0a159) - 2024.03.14

### ♻ Code Refactoring

* defer imports, fix [#610](https://github.com/hpcflow/hpcflow-new/issues/610)
* remove unused sentry dep

### 🐛 Bug Fixes

* tests
* add more `TimeIt` decorators
* add some more `TimeIt.decorator`s
* add some more `TimeIt.decorator`s

### 👷 Build changes

* merge develop


<a name="v0.2.0a158"></a>
## [v0.2.0a158](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a157...v0.2.0a158) - 2024.03.14

### 🐛 Bug Fixes

* use a dict for scheduler `options` and merge options from the config

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a157"></a>
## [v0.2.0a157](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a156...v0.2.0a157) - 2024.03.13

### 🐛 Bug Fixes

* batch up IO in `ZarrPersistentStore._append_parameters` and do not write empty chunks in parameter data base array
* batch up `PendingChanges.commit_parameters` and `commit_param_sources`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a156"></a>
## [v0.2.0a156](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a155...v0.2.0a156) - 2024.03.13

### ✨ Features

* support default values in workflow template string var substitution
* support default values in workflow template string vars

### 🐛 Bug Fixes

* variable substitution various

### 👷 Build changes

* sync upstream GHA templates
* update binary download links file [skip ci]


<a name="v0.2.0a155"></a>
## [v0.2.0a155](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a154...v0.2.0a155) - 2024.03.13

### 🐛 Bug Fixes

* dummy commit to allow release re-run


<a name="v0.2.0a154"></a>
## [v0.2.0a154](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a153...v0.2.0a154) - 2024.03.12

### 🐛 Bug Fixes

* try to fix upload of benchmarks zip artifact

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a153"></a>
## [v0.2.0a153](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a152...v0.2.0a153) - 2024.03.12

### 🐛 Bug Fixes

* try to fix upload of benchmarks zip artifact

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a152"></a>
## [v0.2.0a152](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a151...v0.2.0a152) - 2024.03.12

### 🐛 Bug Fixes

* actually remove 1M elements from benchmark test, takes too long currently


<a name="v0.2.0a151"></a>
## [v0.2.0a151](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a150...v0.2.0a151) - 2024.03.12

### 🐛 Bug Fixes

* remove 1M elements from benchmark test, takes too long currently


<a name="v0.2.0a150"></a>
## [v0.2.0a150](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a149...v0.2.0a150) - 2024.03.12

### ✨ Features

* support workflow template file/string variables with the `--var` CLI option
* add variable substitution to YAML/JSON strings in prep for WorkflowTemplate vars
* add `TimeIt` class to time function pathways

### 🐛 Bug Fixes

* GHA name uploaded artifact at end
* GHA add more options to benchmark
* GHA add python vers as option to benchmark
* GHA update cache vers
* TimeIt and GHA
* benchmark GHA command
* benchmark GHA command

### 👷 Build changes

* sync upstream GHA templates
* sync upstream GHA templates
* update binary download links file [skip ci]


<a name="v0.2.0a149"></a>
## [v0.2.0a149](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a148...v0.2.0a149) - 2024.02.19

### 🐛 Bug Fixes

* fix Python 3.12 support; requires explicit numpy dep

### 👷 Build changes

* sync upstream GHA templates
* bump papermill to fix jupyter tests
* bump test_pre_python to Python 3.13
* bump CI python versions, test on 3.12
* update binary download links file [skip ci]


<a name="v0.2.0a148"></a>
## [v0.2.0a148](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a147...v0.2.0a148) - 2024.01.16

### 🐛 Bug Fixes

* add missing newline
* move profiling to tips and tricks page
* missing new line

### 👷 Build changes

* merge develop
* update binary download links file [skip ci]


<a name="v0.2.0a147"></a>
## [v0.2.0a147](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a146...v0.2.0a147) - 2024.01.04

### ✨ Features

* support passing sub-parameters to `script_data_in`
* add `run_hostname` to EAR

### 🐛 Bug Fixes

* try to fix cancel bug [#572](https://github.com/hpcflow/hpcflow-new/issues/572)
* failing tests


<a name="v0.2.0a146"></a>
## [v0.2.0a146](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a145...v0.2.0a146) - 2023.12.12

### 🐛 Bug Fixes

* fix `get_active_jobscripts` for SLURM when using `max_array_items`; fix [#598](https://github.com/hpcflow/hpcflow-new/issues/598)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a145"></a>
## [v0.2.0a145](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a144...v0.2.0a145) - 2023.12.07

### 🐛 Bug Fixes

* do not filter env executable instances on parallel_mode for now
* fix `add_iteration` with condition action where condition is not met

### 👷 Build changes

* merge develop
* update binary download links file [skip ci]


<a name="v0.2.0a144"></a>
## [v0.2.0a144](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a143...v0.2.0a144) - 2023.11.22

### ✨ Features

* add config item show_command.use_letters
* allow setting nested config items whose root does not exist

### 🐛 Bug Fixes

* format of `Jobscript.task_elements`
* remove unrequired show config item
* update show command iconography to be more accessible; fix [#560](https://github.com/hpcflow/hpcflow-new/issues/560)

### 👷 Build changes

* merge develop
* update binary download links file [skip ci]


<a name="v0.2.0a143"></a>
## [v0.2.0a143](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a142...v0.2.0a143) - 2023.11.22

### 🐛 Bug Fixes

* bug introduced in previous commit

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a142"></a>
## [v0.2.0a142](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a141...v0.2.0a142) - 2023.11.22

### ✨ Features

* support passing rules to `InputFileGenerator`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a141"></a>
## [v0.2.0a141](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a140...v0.2.0a141) - 2023.11.21

### ✨ Features

* support directory demo data in addition to files

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a140"></a>
## [v0.2.0a140](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a139...v0.2.0a140) - 2023.11.20

### 🐛 Bug Fixes

* check for matching env executable instance on submit and fix [#574](https://github.com/hpcflow/hpcflow-new/issues/574)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a139"></a>
## [v0.2.0a139](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a138...v0.2.0a139) - 2023.11.20

### 🐛 Bug Fixes

* simplify `user_runtime_path` and fix [#581](https://github.com/hpcflow/hpcflow-new/issues/581)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a138"></a>
## [v0.2.0a138](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a137...v0.2.0a138) - 2023.11.20

### 🐛 Bug Fixes

* zip and unzip

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a137"></a>
## [v0.2.0a137](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a136...v0.2.0a137) - 2023.11.20

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a136"></a>
## [v0.2.0a136](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a135...v0.2.0a136) - 2023.11.17

### ✨ Features

* add rules attribute to output file parsers

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a135"></a>
## [v0.2.0a135](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a134...v0.2.0a135) - 2023.11.17

### 🐛 Bug Fixes

* use click -- separator to allow for negative exit codes
* quote exit code in case it is negative
* process demo-data files in `InputFile` path
* logical error in `Task.get_available_task_input_sources`
* add_iteration with a default value schema input

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a134"></a>
## [v0.2.0a134](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a133...v0.2.0a134) - 2023.11.13

### ✨ Features

* add `include` option to `ValueSequence.from_rectangle` to optionally exclude edges

### 🐛 Bug Fixes

* failing tests
* fix `._values_from_rectangle` defaults
* build-exes when only build dirs
* add more debug logs again
* build-exes test config
* add more debug logs
* try adding a retry wrapper
* add a debug log

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a133"></a>
## [v0.2.0a133](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a132...v0.2.0a133) - 2023.11.11

### ♻ Code Refactoring

* white space

### ✨ Features

* add `get-active-jobscripts` CLI command

### 🐛 Bug Fixes

* parsing of job ID from SLURM squeue
* fix `Jobscript.get_active_states` for non-job-array scheduled jobscripts
* fix `Submission._set_run_abort`
* remove some test warnings
* expose workflow unzip on CLI

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a132"></a>
## [v0.2.0a132](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a131...v0.2.0a132) - 2023.11.11

### 🐛 Bug Fixes

* actually create alternative runtime directory!

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a131"></a>
## [v0.2.0a131](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a130...v0.2.0a131) - 2023.11.09

### 🐛 Bug Fixes

* groups in loops

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a130"></a>
## [v0.2.0a130](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a129...v0.2.0a130) - 2023.11.09

### ♻ Code Refactoring

* remove unused

### ✨ Features

* option to pass all iterations to a script
* add `ValueSequence.from_rectangle` for points around a rectangular perimeter

### 🐛 Bug Fixes

* fix failing tests
* add local sub-parameters to new iterations
* direct jobscript dependencies; need to commit process ID after each submission
* add loop iteration when a param is sourced from an upstream non-loop task
* object and sub-data retrieval
* get group data that requires merge
* correct `Task.get_input_statuses`
* add `cast_indices=True` in `_get_merged_parameter_data`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a129"></a>
## [v0.2.0a129](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a128...v0.2.0a129) - 2023.11.07

### 🐛 Bug Fixes

* use username rather than user ID in default `alternative_runtime_dir`
* provide a configurable alternative runtime directory for non-standard Linux configurations

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a128"></a>
## [v0.2.0a128](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a127...v0.2.0a128) - 2023.11.06

### ✨ Features

* add `max_array_items` resource option
* add option to choose zarr store compressor

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a127"></a>
## [v0.2.0a127](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a126...v0.2.0a127) - 2023.11.06

### 🐛 Bug Fixes

* make_vers_switcher invocation

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a126"></a>
## [v0.2.0a126](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a125...v0.2.0a126) - 2023.11.06

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a125"></a>
## [v0.2.0a125](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a124...v0.2.0a125) - 2023.11.06

### 🐛 Bug Fixes

* demo data github url sha


<a name="v0.2.0a124"></a>
## [v0.2.0a124](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a123...v0.2.0a124) - 2023.11.06

### ♻ Code Refactoring

* reorg data files

### 🐛 Bug Fixes

* build-exes
* cli command invocation
* set github demo data dir to latest SHA when testing frozen apps in build-exes
* tests

### 👷 Build changes

* merge develop
* update binary download links file [skip ci]


<a name="v0.2.0a123"></a>
## [v0.2.0a123](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a122...v0.2.0a123) - 2023.11.05

### 🐛 Bug Fixes

* demo-data cache via GitHub filesystems in frozen app

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a122"></a>
## [v0.2.0a122](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a121...v0.2.0a122) - 2023.11.05

### 🐛 Bug Fixes

* fix `ParametersList.__getattr__`
* remove temp hardcoded SHA in `BaseApp._get_demo_data_file_source_path`

### 👷 Build changes

* merge upstream python-release-workflow
* update binary download links file [skip ci]


<a name="v0.2.0a121"></a>
## [v0.2.0a121](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a120...v0.2.0a121) - 2023.11.04

### 🐛 Bug Fixes

* try fix [#534](https://github.com/hpcflow/hpcflow-new/issues/534)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a120"></a>
## [v0.2.0a120](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a119...v0.2.0a120) - 2023.11.03

### 🐛 Bug Fixes

* pyinstaller data


<a name="v0.2.0a119"></a>
## [v0.2.0a119](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a118...v0.2.0a119) - 2023.11.03

### ♻ Code Refactoring

* move example data files

### ✨ Features

* substitute <<demo_data_file:FILE_NAME>> in various places
* add initial demo data files support
* add demo example data files and manifest

### 🐛 Bug Fixes

* skip test_run_abort for now on all platforms; too slow
* integration test conditions
* method `BaseApp._run_tests` on Python 3.8
* change pytest invocation method to allow custom args
* sync changes in GHAs
* add __init__.py to demo data dir
* reload template components in `new_null_config` fixture
* try fix test_demo_data
* fix `SchemaInput.from_json_like`
* InputValue repr
* do not package demo data files

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a118"></a>
## [v0.2.0a118](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a117...v0.2.0a118) - 2023.10.31

### ✨ Features

* add unzip

### 👷 Build changes

* merge upstream python-release-workflow


<a name="v0.2.0a117"></a>
## [v0.2.0a117](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a116...v0.2.0a117) - 2023.10.31

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a116"></a>
## [v0.2.0a116](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a115...v0.2.0a116) - 2023.10.26

### ♻ Code Refactoring

* move `get_parameter_names` to `Action`
* move `ElementActionRun.compose_source` to `Action`
* remove unused

### ✨ Features

* support mixed formats in `Action.script_data_in/out`
* add `Workflow.reload`

### 🐛 Bug Fixes

* skip script tests if frozen for now
* add `Action.script_data_files_use_opt`, `Action.save_files`, and `Action.clean_up`
* do not pass CLI option strings for IFGs and OFPs
* add python_env
* get_input_values for single-labelled schema input
* allow passing labelled Task inputs via a dict

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a115"></a>
## [v0.2.0a115](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a114...v0.2.0a115) - 2023.10.13

### ✨ Features

* load workflow from local submission ID
* return Parameter objects that don't exist in ParameterList

### 🐛 Bug Fixes

* failing tests

### 👷 Build changes

* update lock


<a name="v0.2.0a114"></a>
## [v0.2.0a114](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a113...v0.2.0a114) - 2023.10.12

### ✨ Features

* allow selecting a subset of tasks to submit

### 🐛 Bug Fixes

* resolve environments at submit-time; fix [#524](https://github.com/hpcflow/hpcflow-new/issues/524)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a113"></a>
## [v0.2.0a113](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a112...v0.2.0a113) - 2023.10.12

### ♻ Code Refactoring

* Sync workflows and docs with remotes.
* conf.py include only app specific, and imports config_common

### 🐛 Bug Fixes

* replaced head_ref with ref_name to prevent fail if branch is deleted
* test_direct_sub schemas to schema
* "path: resources.os_name"

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a112"></a>
## [v0.2.0a112](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a111...v0.2.0a112) - 2023.10.08

### 🐛 Bug Fixes

* in show, list inactive before unloadable/deleted submissions
* known_subs_file_path should be in user_data_hostname_dir
* only merge template-level resources on initial template creation
* handle exception when getting active jobscripts in show
* update template-component-loading logic to fix tests
* missing arg to `get_enum_by_name_or_val` in `ElementResources`
* incorrect shell vars return from `EAR.compose_command`
* iter method of `Elements` and `Parameters`
* do not create non-existent hyperlink in `TaskSchema._show_info`; fix [#520](https://github.com/hpcflow/hpcflow-new/issues/520)
* copy template-level resources on workflow creation; fix [#461](https://github.com/hpcflow/hpcflow-new/issues/461)
* allow using parameter types that are not pre-defined; finish fix (with previous) [#518](https://github.com/hpcflow/hpcflow-new/issues/518)
* check app data for parameter type in `SchemaInput`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a111"></a>
## [v0.2.0a111](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a110...v0.2.0a111) - 2023.09.25

### ✨ Features

* allow output file parser with no output, for saving files only

### 🐛 Bug Fixes

* IFGs/OFPs when spaces in workflow path
* use MAMBA_EXE first in configure_env

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a110"></a>
## [v0.2.0a110](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a109...v0.2.0a110) - 2023.09.25

### ✨ Features

* support specifying repeats as a single number
* provide default nesting order for upstream inputs

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a109"></a>
## [v0.2.0a109](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a108...v0.2.0a109) - 2023.09.24

### ✨ Features

* support passing arbitrary paths in a <<script:>> pattern
* add rules to TaskSchema.info actions list

### 🐛 Bug Fixes

* failing tests

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a108"></a>
## [v0.2.0a108](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a107...v0.2.0a108) - 2023.09.23

### ✨ Features

* add `Config.add_shell`
* support passing input values in a dict
* copy template file to artifacts dir

### 🐛 Bug Fixes

* test
* submit return indices is now optional - missing change
* submit return indices is now optional

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a107"></a>
## [v0.2.0a107](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a106...v0.2.0a107) - 2023.09.22

### ✨ Features

* add `App.configure_env` and CLI

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a106"></a>
## [v0.2.0a106](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a105...v0.2.0a106) - 2023.09.22

### 🐛 Bug Fixes

* sequence nesting for combining nesting and then parallel-merging
* change `schemas` arg in `Task` to singular `schema`, but store internally as a list

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a105"></a>
## [v0.2.0a105](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a104...v0.2.0a105) - 2023.09.21

### 🐛 Bug Fixes

* add a test skip on macos due to failures that require investigation


<a name="v0.2.0a104"></a>
## [v0.2.0a104](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a103...v0.2.0a104) - 2023.09.21

### ✨ Features

* improve `TaskSchema._show_info`

### 🐛 Bug Fixes

* `SchemaInput.default_value` property
* do not load task schemas when loading app envs

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a103"></a>
## [v0.2.0a103](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a102...v0.2.0a103) - 2023.09.21

### ✨ Features

* add termination condition to Loop

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a102"></a>
## [v0.2.0a102](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a101...v0.2.0a102) - 2023.09.20

### 🐛 Bug Fixes

* test-hpcflow in downstream apps


<a name="v0.2.0a101"></a>
## [v0.2.0a101](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a100...v0.2.0a101) - 2023.09.20

### ✨ Features

* Adds script to configure and sync remotes

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a100"></a>
## [v0.2.0a100](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a99...v0.2.0a100) - 2023.09.19

### 🐛 Bug Fixes

* allow None value in `InputValue._check_dict_value_if_object`

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a99"></a>
## [v0.2.0a99](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a98...v0.2.0a99) - 2023.09.19

### ♻ Code Refactoring

* of `WorkflowTask._get_merged_parameter_data`
* add Rule class

### ✨ Features

* support sourcing labelled inputs from non-labelled upstream tasks
* add `sum()` and `join()` processing functions when formatting command line parameters
* support parsing shell stdout into different data types
* add `TaskSchema.parameter_class_modules` list to specify importable modules container `ParameterValue` classes
* support retrieving `ParameterValue` object properties with e.g. `Element.get`
* allow filtering input sources in the specified task
* use/expect `ElementFilter` in InputSource.where
* allow defining `ActionRule` with `Rule` args

### 🐛 Bug Fixes

* add missing quote
* test
* regex pattern in `Action.get_command_input_types`
* tests for multiple OSes
* improve retrieval of group data when data is unset
* get_merged_parameter_data for a group of PV objects
* add missing sorted
* refine default behaviour if a sub-parameter and root parameter are available as upstream task sources
* do not check dict `InputValue.value` if sub-parameter
* use conditional actions in test failing on posix
* add missing import
* invoc tests
* fix [#501](https://github.com/hpcflow/hpcflow-new/issues/501)
* fix [#500](https://github.com/hpcflow/hpcflow-new/issues/500)

### 👷 Build changes

* merge
* update binary download links file [skip ci]


<a name="v0.2.0a98"></a>
## [v0.2.0a98](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a97...v0.2.0a98) - 2023.09.18

### 🐛 Bug Fixes

* previously added comments about importlib.resources
* previously added comments about importlib.resources

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a97"></a>
## [v0.2.0a97](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a96...v0.2.0a97) - 2023.09.06

### 🐛 Bug Fixes

* add double quotes to path args in bash jobscripts to support spaces
* process spaces in invocation command executable fix [#498](https://github.com/hpcflow/hpcflow-new/issues/498)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a96"></a>
## [v0.2.0a96](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a95...v0.2.0a96) - 2023.09.06

### 🐛 Bug Fixes

* NullScheduler.is_num_cores_supported for unspecified max and step

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a95"></a>
## [v0.2.0a95](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a94...v0.2.0a95) - 2023.09.03

### ✨ Features

* add as_json opt to get_known_submissions
* add ValueSequence.from_geometric_space and from_log_space

### 🐛 Bug Fixes

* App.show always reports submissions as inactive
* add_to_known option in Workflow.submit [#483](https://github.com/hpcflow/hpcflow-new/issues/483)
* try fix [#490](https://github.com/hpcflow/hpcflow-new/issues/490)
* print scheduler submission stderr
* SGEPosix.process_resources
* Config.init path arg if local

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a94"></a>
## [v0.2.0a94](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a93...v0.2.0a94) - 2023.08.30

### 🐛 Bug Fixes

* test_run_abort when slow


<a name="v0.2.0a93"></a>
## [v0.2.0a93](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a92...v0.2.0a93) - 2023.08.30

### ✨ Features

* add abort-run API/CLI

### 🐛 Bug Fixes

* **CLI:** wait command without args

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a92"></a>
## [v0.2.0a92](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a91...v0.2.0a92) - 2023.08.30

### 🐛 Bug Fixes

* reset config test

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a91"></a>
## [v0.2.0a91](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a90...v0.2.0a91) - 2023.08.29

### 🐛 Bug Fixes

* try demo workflow file output in docs

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a90"></a>
## [v0.2.0a90](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a89...v0.2.0a90) - 2023.08.29

### 🐛 Bug Fixes

* demo workflow file output in docs

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a89"></a>
## [v0.2.0a89](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a88...v0.2.0a89) - 2023.08.29

### ♻ Code Refactoring

* user data dirs creation logic; and add user runtime dir
* remove another unused
* remove unused
* remove name from demo workflow_1
* remove unused module
* remove single-use method

### ✨ Features

* add WorkflowTemplate.doc attribute
* add demo-workflow API/CLI

### 🐛 Bug Fixes

* add missing CSS
* BaseApp._load_all_demo_workflows for sphinx config
* App logger reference
* do not use a default template_format (yaml/json)
* do not show test-hpcflow CLI command in hpcflow app
* **config:** import rename option could not be set to False

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a88"></a>
## [v0.2.0a88](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a87...v0.2.0a88) - 2023.08.27

### 🐛 Bug Fixes

* remove option for matflow

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a87"></a>
## [v0.2.0a87](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a86...v0.2.0a87) - 2023.08.27

### Other changes

* update gitignore

### ♻ Code Refactoring

* add config callback_scheduler_set_up
* use existing utils func
* rename config_invocation_key -> config_key
* remove unused
* **config:** use rich for Config._show
* **config:** use ConfigFile._dump for all file dump ops

### ✨ Features

* **config:** add Config.init for easy importing of 'known configs'
* **config:** add make_new arg to Config.import_from_file
* **config:** add rename_config_key
* **config:** use DEFAULT_LOGIN_NODE_MATCH for proper-schedulers to set a reasonable hostname match
* **config:** add ConfigFile.update_invocation
* **config:** allow list of match values
* **config:** add import_from_file
* **config:** add ability to disable temporarily config callbacks
* **config:** add `hpcflow config open` as an alias for `hpcflow open config`
* **schedulers:** add SGE.get_login_nodes and CLI

### 🐛 Bug Fixes

* init ConfigFile before Config, allowing multiple Configs for the same file
* SGE.get_login_nodes newline
* add config callback_scheduler_set_up
* callback_scheduler_set_up
* **config:** remove prints
* **config:** try fix callbacks
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** fix config import CLI
* **config:** fix config import CLI
* **config:** update config import CLI with additional options
* **config:** import_from_file
* **config:** callback_scheduler_set_up
* **config:** try fix
* **config:** try fix
* **config:** try fix
* **config:** tests
* **config:** tests
* **config:** validation for schedulers
* **config:** Config.set

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a86"></a>
## [v0.2.0a86](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a85...v0.2.0a86) - 2023.08.22

### Other changes

* bump deps

### 🐛 Bug Fixes

* remove unused import (tkinter)
* Jobscript.is_array for direct scheduler; maybe affects [#459](https://github.com/hpcflow/hpcflow-new/issues/459) but not sure how
* catch any Workflow._submit exception to fix [#460](https://github.com/hpcflow/hpcflow-new/issues/460)

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a85"></a>
## [v0.2.0a85](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a84...v0.2.0a85) - 2023.08.21

### 🐛 Bug Fixes

* try and test fix for [#467](https://github.com/hpcflow/hpcflow-new/issues/467)
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a84"></a>
## [v0.2.0a84](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a83...v0.2.0a84) - 2023.08.19

### 🐛 Bug Fixes

* always use importlib.resources method for running tests; otherwise we couldn't pass custom CLI options to pytest

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a83"></a>
## [v0.2.0a83](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a82...v0.2.0a83) - 2023.08.19

### 🐛 Bug Fixes

* refactor RunTimeInfo to not use sys.argv on init; fix [#462](https://github.com/hpcflow/hpcflow-new/issues/462)
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing
* **GHA:** testing

### 👷 Build changes

* merge from develop
* bump ipykernel
* update binary download links file [skip ci]


<a name="v0.2.0a82"></a>
## [v0.2.0a82](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a81...v0.2.0a82) - 2023.08.18

### ✨ Features

* prep for tutorial

### 🐛 Bug Fixes

* add set-callback on config item log_console_level to update the handler
* remove AppLog file handlers on reload_config
* improve ConfigItemCallbackError UX

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a81"></a>
## [v0.2.0a81](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a80...v0.2.0a81) - 2023.08.17

### 🐛 Bug Fixes

* bump valida (one valida bug) and fix config validation schema (workaround for another valida bug)

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a80"></a>
## [v0.2.0a80](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a79...v0.2.0a80) - 2023.08.17

### ♻ Code Refactoring

* fix config get calls
* remove unused scheduler_options

### ✨ Features

* support configuring SGE parallel environments and SLURM partitions
* add Config.add_scheduler and CLI
* defer resource validation until submit-time
* support setting/getting config items at dot-delimted paths
* start adding scheduler configuration

### 🐛 Bug Fixes

* print stderr in Scheduler.get_version_info
* SlurmPosix.process_resources typo
* ElementResource.SLURM_is_parallel
* utils.set_in_container
* updated config_schema.yaml
* tests
* use slurm docker root image tag
* pytest run via python -m does not detect custom option, so change to use hpcflow script
* test-scheduler try
* test-scheduler try
* use base slurm image
* test-scheduler try
* test-scheduler try
* SlurmPosix.format_options
* set scheduler to 'direct' if not set in ResourceSpec
* **GHA:** test-shells
* **GHA:** test-shells
* **GHA:** test-shells
* **GHA:** test-shells
* **GHA:** test-shells
* **GHA:** test-shells
* **GHA:** test-shells

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a79"></a>
## [v0.2.0a79](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a78...v0.2.0a79) - 2023.08.15

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a78"></a>
## [v0.2.0a78](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a77...v0.2.0a78) - 2023.08.15

### ✨ Features

* add App.get_config_path and CLI to retrieve config path without loading config

### 🐛 Bug Fixes

* another downstream app hpcflow-test fix
* downstream app test-hpcflow failure; fix https://github.com/hpcflow/matflow-new/issues/89
* set RunTimeInfo.in_pytest in conftest.py

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a77"></a>
## [v0.2.0a77](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a76...v0.2.0a77) - 2023.08.14

### ♻ Code Refactoring

* remove unused
* ResourceList.normalise
* start work to support conditional action based on resources

### ✨ Features

* allow action rules on resources
* **config:** add reset config

### 🐛 Bug Fixes

* disallow ResourceList with multiple identical scopes
* ResourceSpec string scope arg
* passing existing persistent ResourceSpec objects into new workflows/tasks
* merge workflow template resources into element set resources on WorkflowTemplate init
* Workflow.get_all_parameter_data for falsey parameter data
* **config:** account for IPython calling Config.__getattr__
* **config:** reset-config CLI
* **config:** allow resetting the config if it is currently invalid

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a76"></a>
## [v0.2.0a76](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a75...v0.2.0a76) - 2023.08.10

### 🐛 Bug Fixes

* test_in_pytest when frozen


<a name="v0.2.0a75"></a>
## [v0.2.0a75](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a74...v0.2.0a75) - 2023.08.10

### 🐛 Bug Fixes

* skip failing test on non-windows for now
* RunTimeInfo.get_invocation_command within pytest running; fixes [#447](https://github.com/hpcflow/hpcflow-new/issues/447)

### 👷 Build changes

* update binary download links file [skip ci]
* merge from develop


<a name="v0.2.0a74"></a>
## [v0.2.0a74](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a73...v0.2.0a74) - 2023.08.10

### ✨ Features

* add GHA workflow to create a MatFlow PR with bumped hpcflow

### 🐛 Bug Fixes

* new GHA workflow
* new GHA workflow
* new GHA workflow
* new GHA workflow

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a73"></a>
## [v0.2.0a73](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a72...v0.2.0a73) - 2023.08.10

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a72"></a>
## [v0.2.0a72](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a71...v0.2.0a72) - 2023.08.09

### ✨ Features

* simplify ObjectList repr and add TaskSchema.info

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a71"></a>
## [v0.2.0a71](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a70...v0.2.0a71) - 2023.08.03

### 🐛 Bug Fixes

* docs build

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a70"></a>
## [v0.2.0a70](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a69...v0.2.0a70) - 2023.08.03

### ♻ Code Refactoring

* change SchemaInput.accept_multiple to multiple
* add get_enum_by_name_or_val from matflow

### ✨ Features

* support task output labels
* initial support for SchemaInput labels for input parameter multiplicity
* initial support for ElementSet.repeats
* allow action-less schemas for defining inputs

### 🐛 Bug Fixes

* make ElementInputs parameter accessible when they have multiple labels
* set_EAR_end - commit to store at the end so it won't record success on some python failure
* tests that use make_schemas where a default value should not be set
* distinguish SchemaInput default value of None from un-specified
* ValueSequence.values when object parameter is not yet store-committed
* Workflow.batch_update when no pending changes
* sort input_data_idx in _make_new_elements_persistent to ensure correct element data retrieval
* WorkflowTask._get_merged_parameter_data where ParameterValue exists but no class method specified
* get_parameter_data if data is specified falsey

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a69"></a>
## [v0.2.0a69](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a68...v0.2.0a69) - 2023.07.28

### ✨ Features

* rough first implementation of groups
* support merging parameter class method parametrisations from sequences

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a68"></a>
## [v0.2.0a68](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a67...v0.2.0a68) - 2023.07.27

### 👷 Build changes

* sync-updates GH Actions workflow templates
* update binary download links file [skip ci]


<a name="v0.2.0a67"></a>
## [v0.2.0a67](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a66...v0.2.0a67) - 2023.07.26

### 🐛 Bug Fixes

* Python version upper bound
* loosened upper bound for python version in pyproject.toml
* updates lock file to match python version
* .bak file not found in windows,
* makes sed command work with both GNU and BSD/macOS Sed
* Edits python version upper bound to allow 3.12-dev test

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a66"></a>
## [v0.2.0a66](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a65...v0.2.0a66) - 2023.07.24

### 🐛 Bug Fixes

* if parameter value is not a dict, don't try to init a ParameterValue class

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a65"></a>
## [v0.2.0a65](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a64...v0.2.0a65) - 2023.07.20

### ✨ Features

* support running and compiling mtex scripts
* support non-python scripts in actions

### 🐛 Bug Fixes

* failing tests
* add missing func back _resolve_input_source_task_reference
* cast SchemaInput propagation_mode if a string
* add executable arg to Command to allow more control in command formatting

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a64"></a>
## [v0.2.0a64](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a63...v0.2.0a64) - 2023.07.18

### ✨ Features

* nonsense commit to allow version bump


<a name="v0.2.0a63"></a>
## [v0.2.0a63](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a62...v0.2.0a63) - 2023.07.18

### ♻ Code Refactoring

* remove unused

### 🐛 Bug Fixes

* DirectScheduler.wait_for_jobscripts

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a62"></a>
## [v0.2.0a62](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a61...v0.2.0a62) - 2023.07.17

### 🐛 Bug Fixes

* remove TODO

### 👷 Build changes

* merge develop into working


<a name="v0.2.0a61"></a>
## [v0.2.0a61](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a60...v0.2.0a61) - 2023.07.17

### ♻ Code Refactoring

* log EAR states in Jobscript.get_active_states
* log when running subprocess command with SGE/slurm

### ✨ Features

* add show --legend option
* add path option to open workflow command

### 🐛 Bug Fixes

* Slurm get_job_state_info log instead of print; add TODO
* SGE get_job_statuses when no stdout
* correctly raise error in _resolve_workflow_reference if not ID nor path
* sorting of inactive non-deleted submissions in get_known_submissions
* Slurm cancel_jobs command; no --me
* EARStatus check in generate_EAR_resource_map
* EARStatus enum
* App.show table EAR status colours
* only check JS parallelism on Submission.submit, so we can load zip workflows
* EARStatus enum sharing the same value
* SGE qstat wrong user switch; -U doesn't show pending
* cancel_jobs arg name in SGE/slurm
* consistent use of user data dir
* use machine rather than hostname in userdatadir
* jobscript path in scheduler submit command

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a60"></a>
## [v0.2.0a60](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a59...v0.2.0a60) - 2023.07.13

### ✨ Features

* add wait option to workflow submission

### 🐛 Bug Fixes

* use specified number of cores
* select the correct executable instance

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a59"></a>
## [v0.2.0a59](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a58...v0.2.0a59) - 2023.07.04

### ♻ Code Refactoring

* move abort_file variable out of while loop
* move ts_fmt/ts_name_fmt out of creation_info

### ✨ Features

* check exit codes of and redirect stdout/err of JS app invocations
* submit direct jobscripts asynchronously
* add ts_fmt prop to Workflow

### 🐛 Bug Fixes

* don't mutate creation_info
* set jobscript os/shell/scheduler persistently at submit time
* ResourceSpec repr
* lower-case shell/os_name in ResourceSpec init
* method _append_submission_attempts

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a58"></a>
## [v0.2.0a58](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a57...v0.2.0a58) - 2023.06.30

### 🐛 Bug Fixes

* resource sequences

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a57"></a>
## [v0.2.0a57](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a56...v0.2.0a57) - 2023.06.30

### 👷 Build changes

* Updates GH actions workflows and adds test-pre-python.
* update binary download links file [skip ci]
* update GH Actions workflows
* sync-updates GH Actions workflow templates


<a name="v0.2.0a56"></a>
## [v0.2.0a56](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a55...v0.2.0a56) - 2023.06.30

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a55"></a>
## [v0.2.0a55](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a54...v0.2.0a55) - 2023.06.30

### ✨ Features

* improve support for keeping artifact files

### 🐛 Bug Fixes

* log file path in sources
* ValueSequence path validation; do not modify path

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a54"></a>
## [v0.2.0a54](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a53...v0.2.0a54) - 2023.06.29

### ✨ Features

* add scheduler options

### 🐛 Bug Fixes

* jobscript hash
* get jobscript hash
* do not submit with array dep for single element dependencies

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a53"></a>
## [v0.2.0a53](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a52...v0.2.0a53) - 2023.06.29

### 🐛 Bug Fixes

* SGE stdout job ID parsing for job arrays

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a52"></a>
## [v0.2.0a52](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a51...v0.2.0a52) - 2023.06.29

### 🐛 Bug Fixes

* changed store.features to _features

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a51"></a>
## [v0.2.0a51](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a50...v0.2.0a51) - 2023.06.28

### ♻ Code Refactoring

* use flat global lists for elements, iter, runs

### 👷 Build changes

* merge from develop
* update binary download links file [skip ci]


<a name="v0.2.0a50"></a>
## [v0.2.0a50](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a49...v0.2.0a50) - 2023.06.20

### 🐛 Bug Fixes

* Multiline output
* Multiline output
* update pyinstaller to prevent error "pyimod02_importers is NULL!"
* update pyinstaller to prevent errowr "pyimod02_importers is NULL!"
* Deprecation warnings - Multiline outputs
* multiline output with GITHUB_OUTPUT method

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a49"></a>
## [v0.2.0a49](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a48...v0.2.0a49) - 2023.06.06

### 🐛 Bug Fixes

* deprecation warnings & GHA python version bump
* bumped python version in GHA environment variables.
* replaced deprecated set-output

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a48"></a>
## [v0.2.0a48](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a47...v0.2.0a48) - 2023.05.31

### ♻ Code Refactoring

* removed duplicate declaration of click options

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a47"></a>
## [v0.2.0a47](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a46...v0.2.0a47) - 2023.05.30

### 🐛 Bug Fixes

* remove failing test due to recent change
* check_valid_py_identifier does not lower-case; improve docstring; add sphinx docs; add tests
* check_valid_py_identifier in Loop init
* check for bad DotAccessObjectList access attr name and tidy check_valid_py_identifier docstring

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a46"></a>
## [v0.2.0a46](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a45...v0.2.0a46) - 2023.05.30

### ♻ Code Refactoring

* remove debug print

### 🐛 Bug Fixes

* numpy DeprecationWarning
* invalid escape sequence
* revert "fix: see if removing line-cont. backslash fixes invalid escape seq."
* see if removing line-cont. backslash fixes invalid escape seq.
* try to remove invalid escape sequence warning in config
* importlib.resources deprecation warnings on 3.11
* load_config warnings in tests

### 👷 Build changes

* bump colorama
* bump zarr
* update binary download links file [skip ci]


<a name="v0.2.0a45"></a>
## [v0.2.0a45](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a44...v0.2.0a45) - 2023.05.28

### 🐛 Bug Fixes

* command --hpcflow-version

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a44"></a>
## [v0.2.0a44](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a43...v0.2.0a44) - 2023.05.28

### 👷 Build changes

* merge from develop


<a name="v0.2.0a43"></a>
## [v0.2.0a43](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a42...v0.2.0a43) - 2023.05.28

### ♻ Code Refactoring

* add file_schema attribute to ConfigFile, add _dump_config

### ✨ Features

* add docs_import_conv to BaseApp
* add _validation attribute to Parameter to assign a valida Schema
* support sorting Parameter
* add App.get_parameter_task_schema_map func to map param types to task schemas

### 🐛 Bug Fixes

* pip install dist name
* **GHA:** add app_name variable
* **docs:** ensure _generated dir exists

### 👷 Build changes

* merge from develop
* revert "bump: 0.2.0a42 → 0.2.0a43 [skip ci]"
* merge from develop
* revert "build: update binary download links file [skip ci]"
* update binary download links file [skip ci]
* merge from develop
* add myself as .github owner?
* revert "bump: 0.2.0a42 → 0.2.0a43 [skip ci]"
* update deps
* update binary download links file [skip ci]


<a name="v0.2.0a42"></a>
## [v0.2.0a42](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a41...v0.2.0a42) - 2023.05.20

### Other changes

* merge branch develop into refactor/app

### 🐛 Bug Fixes

* ElementPrefixedParameter app access attr
* InputFileGenerate/OutputFileParser.compose_source
* Submission must be an app class as well - parent ref in jobscript

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a41"></a>
## [v0.2.0a41](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a40...v0.2.0a41) - 2023.05.20

### ♻ Code Refactoring

* initial refactor of app structure; load attrs dynamically

### 🐛 Bug Fixes

* more fixes to support the downstream app
* fixes to support the downstream app
* ParameterValue subclass check
* **pyi:** add hidden imports

### 👷 Build changes

* update deps
* update binary download links file [skip ci]


<a name="v0.2.0a40"></a>
## [v0.2.0a40](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a39...v0.2.0a40) - 2023.05.17

### 🐛 Bug Fixes

* WorkflowTemplate._from_data ensure an element set: include all task arguments

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a39"></a>
## [v0.2.0a39](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a38...v0.2.0a39) - 2023.05.16

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a38"></a>
## [v0.2.0a38](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a37...v0.2.0a38) - 2023.05.16

### Other changes

* merge branch fix/poetry-pre-commit into feat/config

### ♻ Code Refactoring

* remove unused module
* move api.py out of sub-package
* remove unused
* don't use Config private methods
* change RunTimeInfo.machine -> hostname to disambiguate from user-specified machine in config
* write default config file using \n line endings

### ✨ Features

* support variables in config
* add RunTimeInfo.in_ipython
* config match run-time info with optional glob patterns

### 🐛 Bug Fixes

* logging; fix [#307](https://github.com/hpcflow/hpcflow-new/issues/307)
* RunTimeInfo repr

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a37"></a>
## [v0.2.0a37](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a36...v0.2.0a37) - 2023.05.16

### 🐛 Bug Fixes

* **gha:** update poetry check pre-commit rev

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a36"></a>
## [v0.2.0a36](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a35...v0.2.0a36) - 2023.05.05

### ✨ Features

* dummy commit


<a name="v0.2.0a35"></a>
## [v0.2.0a35](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a34...v0.2.0a35) - 2023.05.05

### Other changes

* whitespace
* whitespace
* whitespace
* merge branch develop into feat/plugins

### ♻ Code Refactoring

* move cli.py to be consistent with matflow
* remove unused
* tidy task/element artifact dir names

### ✨ Features

* add template_components CLI and suppress runtimeinfo warning
* support invoking ParameterValue class methods via JSON-like input value
* support masked arrays in zarr
* support main script in action
* add Workflow.save_parameters
* add scripts_dir arg to App
* support IPG and OFPs in submission.
* add __iter__ to _ElementPrefixedParameter

### 🐛 Bug Fixes

* tests
* update pyi make files
* update _DummyPersistentWorkflow so it can retrieve data indicies
* run time info should use package name when in ipython
* Zarr store _encode_numpy_array
* EAR.get_resources to include template-level if not in EAR-level
* distinguish app name from package name
* use zero-indexed JS_elem_idx in bash
* name command files by action index

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a34"></a>
## [v0.2.0a34](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a33...v0.2.0a34) - 2023.05.04

### 🐛 Bug Fixes

* change default config dir so it does not conflict with old install

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a33"></a>
## [v0.2.0a33](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a32...v0.2.0a33) - 2023.05.03

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a32"></a>
## [v0.2.0a32](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a31...v0.2.0a32) - 2023.05.02

### 🐛 Bug Fixes

* repair poetry.lock
* missing [[package]] in poetry.lock

### 👷 Build changes

* update binary download links file [skip ci]
* update deps
* update python version constraint (3.8 to 3.11)


<a name="v0.2.0a31"></a>
## [v0.2.0a31](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a30...v0.2.0a31) - 2023.05.01

### Other changes

* merge branch fix/examples into feat/submission
* merge branch 'develop' into feat/submission
* merge branch 'develop' into feat/submission
* start work on generating jobscripts

### ♻ Code Refactoring

* remove unused
* remove unused
* remove unused var
* remove unused functions and tests
* load config on demand if not loaded
* remove unused BaseApp property
* define ALL_STORE_FORMATS and DEFAULT_STORE_FORMAT
* define ElementID, IterationID, and EAR_ID
* separate out EAR initialisation
* remove unused metadata properties
* intial extraction of EAR initialisation
* start support for loops (JSON persistent store only)

### ✨ Features

* add simple EAR status printout
* specify submission dir for sge out/err logs
* specify submission dir for slurm out/err logs
* specify slurm out/err file names
* print number of jobscripts submitted
* add workflow submission/jobscript CLI
* add basic parameter getting to CLI
* add shell+OS version info to jobscript submission
* use artifacts path and get WSL submission working
* store creation_info (app/python version) in metadata
* add PersistentStoreFeatures class to be used on submission
* in initialise_EARs, return indicies of iterations for which EARs were initialised
* support powershell submission
* add Shell classes
* allow specifying config invocation key
* add environment setup to JS app invocation func
* add machine (socket.hostname) to run-time info
* add more Workflow/Template creation class methods and reflect changes in CLI/API
* add Workflow(Template).from_JSON_file/string and WorkflowTemplate.from_file
* allow specifying template components in the template file; fix [#373](https://github.com/hpcflow/hpcflow-new/issues/373)
* add Workflow.from_YAML_string
* add initial workflow creation CLI
* enable jobscript execution
* support simple loops in submission
* initial implementation of Workflow.submit()
* support template-level resources
* add WorkflowLoop.add_iteration
* add Element.get_dependent_elements_recursively
* flesh out/validate Loop object
* track index of original input source for each element input
* initial Workflow.resolve_jobscripts method
* add set_EAR_start/end methods to Workflow
* add metadata to EARs
* add raise_on_unset bool arg to _get_merged_parameter_data
* **CLI:** allow passing on arbitrary pytest arguments to test CLI

### 🐛 Bug Fixes

* tests
* return from make_and_submit_workflow
* get_OS_info_POSIX on linux
* js-parallelism cli option
* check if store type support jobscript parallelism
* check if store type support EAR parallelism
* don't modify run_time_info invocation command!
* is_array should be False when no scheduler
* remove debug prints
* split JSONPersistentStore over three JSON files to support schedulers
* shebang in jobscripts
* cannot use Literal type in py3.7/8
* remove debug prints
* get_invocation_command when in ipython/jupyter; fix [#374](https://github.com/hpcflow/hpcflow-new/issues/374)
* json persistent store is_modified_check should not check non-metadata
* compose_jobscript env_setup
* define a bash function for app invocation
* jobscript naming for SGE (no numbers at start)
* Submission._raise_failure exception message
* SGE get_version_info
* make SGE cwd_switch arg optional
* exception message
* PersistentStore get_submissions to cast str keys in dict
* be explicit about API methods to add to App
* Loop_.json_like_constructor for reading from YAML file
* get_submissions mutates zarr metadata
* EAR CLI click args
* still incorrect save-parameter command in write_commands
* incorrect save-parameter command in write_commands
* missing newline in compose_commands
* bug another in Workflow.write_commands
* bug in Workflow.write_commands
* jobscript submit time record bugs
* get_EARs_from_IDs bad args
* case of args in CLI
* expand aliases in bash jobscripts
* CLI submit return code
* func args in parse_submission_output
* parse_submission_output in slurm
* None check
* resolve Workflow path
* add submit command to JobscriptSubmissionFailure
* not None checks
* jobscript version info
* bash string comparison
* accept store in make CLI
* pass is_array to Scheduler.format_options
* use resources from zeroth iteration in subsequent iterations
* parameter source bugs in JSON store
* default task input source: outputs take precedence
* default task input source chosen should be nearest
* correct make_persistent method return types
* improve ElementSet input_sources validation
* improve ElementSet inputs/sequences validatation
* ValueSequence 'normalised_inputs_path' before _parameter is assigned
* sub-parameters now appear in ElementSet.input_sources
* sub-parameters now appear in EAR data_idx when they are in schema-level data_idx
* keys ordering in Action.generate_data_index to ensure processing works as intended
* EAR iteration index refers to position in element_iterations container!
* update param source for schema outputs in generate_data_index
* generate_data_index
* prepare_persistent_outputs cannot know EAR idx until initialise_EARs
* iteration index in Zarr store
* ElementerIteration index is now correct
* pass on pytest return code to API/CLI
* support element iterations in Zarr store
* Zarr store set_parameter
* test_action_rule: workaround for custom rule testing (see https://github.com/hpcflow/valida/issues/9)
* JSONPersistentStore: explicitly cast parameter data indices to strings on commit
* test_action_rule for custom rule
* various fixes for Element -> ElementIteration (JSON persistent only)

### 👷 Build changes

* merge branch develop into feat/submission
* update poetry lock
* merge branch feat/loops into feat/submission


<a name="v0.2.0a30"></a>
## [v0.2.0a30](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a29...v0.2.0a30) - 2023.05.01

### ✨ Features

* refine element propagation

### 🐛 Bug Fixes

* partially fix example.ipynb, add_elements won't work until feat/submission is merged
* WorkflowTask._add_elements should pass on propagate_to


<a name="v0.2.0a29"></a>
## [v0.2.0a29](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a28...v0.2.0a29) - 2023.04.25

### ♻ Code Refactoring

* split add_tassk_before_and_after into unit test

### ✨ Features

* added docstrings for add_task before and after
* defined add_task_after and add_task_before, with test

### 🐛 Bug Fixes

* default task index, new task variable name and pass.


<a name="v0.2.0a28"></a>
## [v0.2.0a28](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a27...v0.2.0a28) - 2023.04.20

### ✨ Features

* Update ubuntu runners for build docs

### 🐛 Bug Fixes

* Correct typo in Configure Poetry in release

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a27"></a>
## [v0.2.0a27](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a26...v0.2.0a27) - 2023.04.19


<a name="v0.2.0a26"></a>
## [v0.2.0a26](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a25...v0.2.0a26) - 2023.03.28

### 👷 Build changes

* Update relase actions workflow.
* Update build-exes workflow
* Update build-exes workflow
* Updated actions workflows
* Updated all actions workflows
* update binary download links file [skip ci]


<a name="v0.2.0a25"></a>
## [v0.2.0a25](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a24...v0.2.0a25) - 2023.03.22

### ♻ Code Refactoring

* removed InputSourceMode


<a name="v0.2.0a24"></a>
## [v0.2.0a24](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a23...v0.2.0a24) - 2023.03.06

### ✨ Features

* store element meta-data in zarr array

### 🐛 Bug Fixes

* add missing import to pyinstsaller hook
* Task.insert_ID is now unique!
* get_events_of_type now considers unsaved events

### 👷 Build changes

* merge develop
* merge develop
* update binary download links file [skip ci]
* merge develop
* merge from remote


<a name="v0.2.0a23"></a>
## [v0.2.0a23](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a22...v0.2.0a23) - 2023.02.09

### ✨ Features

* allow passing input files (fix [#345](https://github.com/hpcflow/hpcflow-new/issues/345)); also add rules to actions
* add Element.test_rule to test a Rule on an element's data

### 👷 Build changes

* bump deps + remove helper group
* update binary download links file [skip ci]


<a name="v0.2.0a22"></a>
## [v0.2.0a22](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a21...v0.2.0a22) - 2023.01.26

### ✨ Features

* example notebook

### 🐛 Bug Fixes

* add missing class to __init__
* fix getting correct group index when from a task input

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a21"></a>
## [v0.2.0a21](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a20...v0.2.0a21) - 2023.01.23

### ♻ Code Refactoring

* Moved helper dependencies to poetry.group.helper.dependencies

### 👷 Build changes

* poetry lock
* update binary download links file [skip ci]


<a name="v0.2.0a20"></a>
## [v0.2.0a20](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a19...v0.2.0a20) - 2023.01.20

### ♻ Code Refactoring

* remove duplicate core_classes
* improve get_available_task_input_sources
* improve InputSource repr
* remove unused
* remove unused
* remove unused Task arguments for now
* remove unused
* remove unused method

### ✨ Features

* add notebook to look at element dependency viz
* propagate new elements to downstream tasks
* Adds app name to --version option to cli help.
* add Workflow.copy method
* add sourceable_elements option to ElementSet
* track sequence indices for each element
* track element set for each element
* support passing a base element to add_elements
* support passing already-bound inputs, resources and sequences to add_elements
* track input sources for each new element
* add_elements
* raise on missing inputs

### 🐛 Bug Fixes

* add hidden import to pyinstaller
* decorator order matters for staticmethods!
* combining upstream parameter with locally defined sub-parameter
* track input sources for sub-parameters
* local values take precedence over other sources
* allow passing SchemaInput to TaskSchema outputs arg
* no need to specify 'name' in schema objective YAML
* BaseApp.shared_data_from_json_like to not use app_data

### 👷 Build changes

* merge
* update binary download links file [skip ci]


<a name="v0.2.0a19"></a>
## [v0.2.0a19](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a18...v0.2.0a19) - 2022.12.22

### ♻ Code Refactoring

* remove unused imports; sort

### ✨ Features

* move clear_helper to API
* add WorkflowMonitor.on_modified event to log
* more helper functions + use PollingObserver to work on networked file systems
* add watchdog to helper
* add timeout option to helper
* add clear command to helper CLI
* add pid command to helper CLI
* initial server implementation
* expose make/submit-workflow to CLI
* add Workflow.submit for local serial execution
* add demo-software CLI
* add scripts app data and first demo scripts
* validate inputs and sequence paths
* update SchemaInput repr
* add resources property to Element

### 🐛 Bug Fixes

* another py37 f-string
* py37 fstring incompat
* helper watch-list if not running
* delete watch file in stop_helper
* missing f-string
* merge branch 'aplowman/develop' into feat/server
* App._load_scripts for frozen app
* helper invocation on frozen
* ValueSequence cls methods
* quote script paths
* add missing WorkflowTask.dir_path prop
* InputFile
* ValueSequence repr
* EnvironmentList.get when no specifiers
* element resolution of downstream tasks
* Element.get with no path arg
* set unsourced inputs
* sequence path cannot start with output!
* add _validate_parameter_path to ValueSequence
* use dot-delimited string for Element.get() path arg

### 👷 Build changes

* merge
* update binary download links file [skip ci]
* merge branch 'aplowman/develop' into feat/server
* merge branch 'develop' into aplowman/develop
* add watchdog dep


<a name="v0.2.0a18"></a>
## [v0.2.0a18](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a17...v0.2.0a18) - 2022.12.12

### Other changes

* Add wf to main so appears in API
* Add wf to main so appears in API

### 🐛 Bug Fixes

* Update wf
* Make capitalisation consistent.
* Make capitalisation consistent.
* onedir output now linked correctly

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a17"></a>
## [v0.2.0a17](https://github.com/hpcflow/hpcflow-new/compare/v0.0.1...v0.2.0a17) - 2022.12.09

### Other changes

* tweak powershell compress
* test bash compress
* Fix filenames in release testing
* correct error in release_testing
* Update permissions on compress script
* fix filenames in release_testing
* Change to compress script (bash)
* re-add release_testing.yaml
* Update permissions on compress (bash)
* remove extra space
* Remove test workflows.
* rectify conflicting action ids
* test exisitng *win-dir.zip
* add renamed archive to release
* change compressed filename
* Test separate compress script
* tweaking if statement again
* tweak if statement
* fix if statement
* test zip win-dir right after build
* test zip on macOS
* tweak zip command
* remove extra space
* test zip win-dir before release
* test download to action root
* test macOS-dir in isolation
* Try download win-dir only
* Try artefact path beginning ./
* fix path to artifacts
* test specify full path to artefacts
* test softprops/action-gh-release[@v0](https://github.com/v0).1.13
* fix artefact names in relese step
* Add workflow to test GH releasing

### ♻ Code Refactoring

* add App.assign_core_classes
* remove unused import
* Remove multiprocessing.freeze_support

### ✨ Features

* change package name temporarily
* add optional _json_like_constructor method
* add testing to API and CLI
* initial pass of zarr integration
* support ChildObjectSpec within dict values
* JSONLike superclass
* add python_version to run time info and Sentry tag
* add sentry tracing
* improve configuration feature
* pass version to RunTimeInfo
* add --run-time-info option to CLI
* add logging
* test
* **gha:** Add pyinstaller onedir to release

### 🐛 Bug Fixes

* pyinstaller build scripts
* Correct typo in linux onedir upload name
* Correct typo in release build windows, file
* Remove .zip from onedir filenames
* Correct typo in Version check (win, folder).
* Correct typo in Version check (win, folder).
* Correct erroneous version number.
* Remove erroneously updated changelog.
* Remove erroneously updated changelog.
* Fix typo in release workflow template
* Fix incorrect folder for pyinst onefolder
* workflow dep
* Correct typos in make script
* Correct typo in build-exes workflow
* Fix typo in build-exes template & workflow
* show config dir metadata on ConfigValidationError
* Fix typo in pyinstaller/make.sh
* Change onefolder to onefir in makefiles
* Change onefolder to onedir in makefiles
* invoke tests from non-frozen
* Fix name of release_testing wf.
* scoping issue when adding multiple API methods to the BaseApp
* init of InputValue with array value
* retrieval of Task inputs once workflow-bound
* GHA git perms
* scripts
* more unit tests
* failing tests
* to_json_like round trip test with parent ref
* actions workflow
* more config fixes
* update config valida schema
* default config
* add pyinstaller hidden imports
* test config CLI
* better config
* overriding log file from CLI
* **build:** GHA poetry install commands
* **gha:** Correct typo in build wf file & template
* **gha:** Fix typo in build-exes wf temp & file

### 👷 Build changes

* repeat release
* Add compress script (bash)
* Add compress script (powershell)
* update binary download links file [skip ci]
* update binary download links file [skip ci]
* Update pyinstaller make scripts
* slightly increase black line-length
* update poetry lock
* add scripts for sphinx-apidoc
* add auto-gen requirements
* pre-commits
* poetry update again
* Update make scripts.
* remove unused ci pre-commit
* update binary download links file [skip ci]
* merge
* Update make scripts
* clean before pyinstaller run
* update binary download links file [skip ci]
* merge
* Add sep folder for onefile, onefolder build
* Add onefile build to ipyinstaller makefiles
* update binary download links file [skip ci]
* update deps
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* update binary download links file [skip ci]
* fix entrypoint
* update binary download links file [skip ci]
* fix pyinstaller entry in actions
* merge develop
* fix pyinstaller scripts
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* move tests to unit dir
* add pyi hidden import
* deps
* update binary download links file [skip ci]
* update binary download links file [skip ci]
* add empty init py in tests
* update binary download links file [skip ci]
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add back sdk data hiddenimport!
* merge develop
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add empty init file back to sdk data
* try to fix mac problem when using SDK
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* remove unused 'include'
* add test extra
* use poetry dep groups
* update binary download links file [skip ci]
* update deps
* allow tests to run frozen
* merge develop
* update binary download links file [skip ci]
* Fix failing test workflow on fork
* try pyinstaller fix
* merge from develop
* update binary download links file [skip ci]
* try add hidden import
* merge
* update poetry
* update binary download links file [skip ci]
* update GHA workflows
* add missing data files
* update GH Actions workflows
* update binary download links file [skip ci]
* merge
* merge from develop
* fix pyinstaller build on MacOS
* more pyinstaller tweaks
* update pyinstaller hooks
* update poetry pre-commit
* update GH workflows
* update gitignore
* merge
* update deps
* update binary download links file [skip ci]
* workflows
* add pyinstaller log level actions input
* use pyinstaller collect_data_files
* chmod+x pyinstaller make
* use pyinstaller hook
* merge
* update binary download links file [skip ci]
* CI issue https://github.com/psf/black/issues/2964
* update GH workflow cache keys
* remove debug print
* test on docker image as well
* try add tkinter import 2
* try add hidden import again
* updated poetry lock
* add hidden import for linux pyinstaller
* **GHA:** Update workflow templates and files
* **GHA:** update os vers
* **GHA:** remove pytest module restriction
* **GHA:** run tests on frozen app
* **GHA:** update py vers
* **GHA:** template updates
* **GHA:** don't run test on push
* **GHA:** Update build workflow template & file
* **GHA:** Update workflow templates
* **GHA:** Update workflow YAML files
* **gha:** Update build workflow file & template
* **gha:** Update release & build-exes workflows.
* **gha:** Compress onedir output for release
* **gha:** Update build-exes template & workflow
* **gha:** Update release & build-exes workflows
* **gha:** Update build-exes wf template and file
* **gha:** Update build-exes wf template and file
* **gha:** Update release wf with pyinst ondir
* **gha:** Update pyinstaller make files
* **gha:** Update release & build-exes workflows
* **gha:** Update build-exes wf template & file
* **pyi:** fix custom hook
* **pyinstaller:** Update pyinstaller make files
* **workflow:** Fix failing test wokflow on fork


<a name="v0.0.1"></a>
## [v0.0.1](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a16...v0.0.1) - 2022.12.09

### Other changes

* add workflow to main to enable running


<a name="v0.2.0a16"></a>
## [v0.2.0a16](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a15...v0.2.0a16) - 2022.11.28

### ✨ Features

* **gha:** Add pyinstaller onedir to release

### 🐛 Bug Fixes

* Correct typo in linux onedir upload name
* Correct typo in release build windows, file
* Remove .zip from onedir filenames
* Correct typo in Version check (win, folder).
* Correct typo in Version check (win, folder).
* Correct erroneous version number.
* Remove erroneously updated changelog.
* Remove erroneously updated changelog.
* Fix typo in release workflow template
* Fix incorrect folder for pyinst onefolder
* Change onefolder to onefir in makefiles
* Correct typos in make script
* Correct typo in build-exes workflow
* Fix typo in build-exes template & workflow
* Fix typo in pyinstaller/make.sh
* Change onefolder to onedir in makefiles
* **gha:** Correct typo in build wf file & template
* **gha:** Fix typo in build-exes wf temp & file

### 👷 Build changes

* Update make scripts.
* Add onefile build to ipyinstaller makefiles
* Update pyinstaller make scripts
* Add sep folder for onefile, onefolder build
* Update make scripts
* update binary download links file [skip ci]
* update binary download links file [skip ci]
* **GHA:** Update build workflow template & file
* **GHA:** Update workflow templates and files
* **gha:** Update build-exes wf template and file
* **gha:** Update release & build-exes workflows
* **gha:** Update build-exes wf template & file
* **gha:** Update build-exes template & workflow
* **gha:** Update release wf with pyinst ondir
* **gha:** Update build workflow file & template
* **gha:** Update pyinstaller make files
* **gha:** Update build-exes wf template and file
* **gha:** Update release & build-exes workflows.
* **gha:** Update release & build-exes workflows
* **pyinstaller:** Update pyinstaller make files


<a name="v0.2.0a15"></a>
## [v0.2.0a15](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a14...v0.2.0a15) - 2022.11.14

### ♻ Code Refactoring

* add App.assign_core_classes

### ✨ Features

* add optional _json_like_constructor method

### 👷 Build changes

* update binary download links file [skip ci]
* **GHA:** Update workflow YAML files
* **GHA:** Update workflow templates


<a name="v0.2.0a14"></a>
## [v0.2.0a14](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a13...v0.2.0a14) - 2022.10.30

### 👷 Build changes

* update binary download links file [skip ci]
* **pyi:** fix custom hook


<a name="v0.2.0a13"></a>
## [v0.2.0a13](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a12...v0.2.0a13) - 2022.10.30

### 👷 Build changes

* deps
* update binary download links file [skip ci]
* add pyi hidden import
* **GHA:** update py vers
* **GHA:** run tests on frozen app
* **GHA:** remove pytest module restriction
* **GHA:** update os vers
* **GHA:** template updates


<a name="v0.2.0a12"></a>
## [v0.2.0a12](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a11...v0.2.0a12) - 2022.10.29

### 🐛 Bug Fixes

* invoke tests from non-frozen

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a11"></a>
## [v0.2.0a11](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a10...v0.2.0a11) - 2022.10.29

### 👷 Build changes

* add empty init py in tests
* update binary download links file [skip ci]


<a name="v0.2.0a10"></a>
## [v0.2.0a10](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a9...v0.2.0a10) - 2022.10.29

### 🐛 Bug Fixes

* **build:** GHA poetry install commands

### 👷 Build changes

* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add back sdk data hiddenimport!
* merge develop
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add empty init file back to sdk data
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* remove unused 'include'
* add test extra
* use poetry dep groups
* update binary download links file [skip ci]
* **GHA:** don't run test on push


<a name="v0.2.0a9"></a>
## [v0.2.0a9](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a8...v0.2.0a9) - 2022.10.29

### ♻ Code Refactoring

* remove unused import

### ✨ Features

* add testing to API and CLI

### 🐛 Bug Fixes

* scoping issue when adding multiple API methods to the BaseApp
* init of InputValue with array value
* retrieval of Task inputs once workflow-bound

### 👷 Build changes

* update deps
* allow tests to run frozen
* merge develop
* update binary download links file [skip ci]
* merge from develop


<a name="v0.2.0a8"></a>
## [v0.2.0a8](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a7...v0.2.0a8) - 2022.10.25

### ♻ Code Refactoring

* Remove multiprocessing.freeze_support

### 👷 Build changes

* Fix failing test workflow on fork
* update binary download links file [skip ci]
* **workflow:** Fix failing test wokflow on fork


<a name="v0.2.0a7"></a>
## [v0.2.0a7](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a6...v0.2.0a7) - 2022.10.19

### 🐛 Bug Fixes

* GHA git perms

### 👷 Build changes

* try to fix mac problem when using SDK
* merge
* update poetry
* update binary download links file [skip ci]


<a name="v0.2.0a6"></a>
## [v0.2.0a6](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a5...v0.2.0a6) - 2022.10.19

### 👷 Build changes

* update GHA workflows
* add missing data files
* update GH Actions workflows
* update binary download links file [skip ci]


<a name="v0.2.0a5"></a>
## [v0.2.0a5](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a4...v0.2.0a5) - 2022.10.19

### ✨ Features

* initial pass of zarr integration
* support ChildObjectSpec within dict values
* JSONLike superclass
* add python_version to run time info and Sentry tag

### 🐛 Bug Fixes

* scripts
* more unit tests
* failing tests
* to_json_like round trip test with parent ref

### 👷 Build changes

* merge
* merge from develop
* fix pyinstaller build on MacOS
* more pyinstaller tweaks
* update pyinstaller hooks
* update poetry pre-commit
* updated poetry lock
* update gitignore
* merge
* update deps
* update binary download links file [skip ci]


<a name="v0.2.0a4"></a>
## [v0.2.0a4](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a3...v0.2.0a4) - 2022.03.30

### 🐛 Bug Fixes

* actions workflow
* more config fixes
* update config valida schema
* default config
* add pyinstaller hidden imports
* test config CLI

### 👷 Build changes

* workflows
* add pyinstaller log level actions input
* use pyinstaller collect_data_files
* chmod+x pyinstaller make
* use pyinstaller hook
* merge
* update binary download links file [skip ci]


<a name="v0.2.0a3"></a>
## [v0.2.0a3](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a2...v0.2.0a3) - 2022.03.29

### ✨ Features

* add sentry tracing
* improve configuration feature
* pass version to RunTimeInfo

### 🐛 Bug Fixes

* better config
* overriding log file from CLI
* show config dir metadata on ConfigValidationError
* workflow dep
* pyinstaller build scripts

### 👷 Build changes

* CI issue https://github.com/psf/black/issues/2964
* update GH workflow cache keys
* remove debug print
* test on docker image as well
* try add tkinter import 2
* try add hidden import again
* try add hidden import
* add hidden import for linux pyinstaller
* update GH workflows
* try pyinstaller fix
* move tests to unit dir
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* fix pyinstaller scripts
* merge develop
* fix pyinstaller entry in actions
* fix entrypoint
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* update deps
* merge
* update binary download links file [skip ci]


<a name="v0.2.0a2"></a>
## [v0.2.0a2](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a1...v0.2.0a2) - 2022.03.21

### ✨ Features

* add --run-time-info option to CLI

### 👷 Build changes

* clean before pyinstaller run
* merge
* update binary download links file [skip ci]
* remove unused ci pre-commit


<a name="v0.2.0a1"></a>
## [v0.2.0a1](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a0...v0.2.0a1) - 2022.03.21

### ✨ Features

* add logging

### 👷 Build changes

* poetry update again
* pre-commits
* add auto-gen requirements
* add scripts for sphinx-apidoc
* update poetry lock
* slightly increase black line-length
* update binary download links file [skip ci]


<a name="v0.2.0a0"></a>
## [v0.2.0a0](https://github.com/hpcflow/hpcflow-new/compare/v0.1.16...v0.2.0a0) - 2022.03.18

### ✨ Features

* test
* change package name temporarily
* revert version

### 👷 Build changes

* repeat release
* initial setup


<a name="v0.1.16"></a>
## [v0.1.16](https://github.com/hpcflow/hpcflow-new/compare/v0.1.15...v0.1.16) - 2021.06.06


<a name="v0.1.15"></a>
## [v0.1.15](https://github.com/hpcflow/hpcflow-new/compare/v0.1.14...v0.1.15) - 2021.04.10


<a name="v0.1.14"></a>
## [v0.1.14](https://github.com/hpcflow/hpcflow-new/compare/v0.1.13...v0.1.14) - 2021.02.05


<a name="v0.1.13"></a>
## [v0.1.13](https://github.com/hpcflow/hpcflow-new/compare/v0.1.12...v0.1.13) - 2021.01.18


<a name="v0.1.12"></a>
## [v0.1.12](https://github.com/hpcflow/hpcflow-new/compare/v0.1.11...v0.1.12) - 2020.12.16


<a name="v0.1.11"></a>
## [v0.1.11](https://github.com/hpcflow/hpcflow-new/compare/v0.1.10...v0.1.11) - 2020.08.25


<a name="v0.1.10"></a>
## [v0.1.10](https://github.com/hpcflow/hpcflow-new/compare/v0.1.9...v0.1.10) - 2020.07.07


<a name="v0.1.9"></a>
## [v0.1.9](https://github.com/hpcflow/hpcflow-new/compare/v0.1.8...v0.1.9) - 2020.06.09


<a name="v0.1.8"></a>
## [v0.1.8](https://github.com/hpcflow/hpcflow-new/compare/v0.1.7...v0.1.8) - 2020.06.09


<a name="v0.1.7"></a>
## [v0.1.7](https://github.com/hpcflow/hpcflow-new/compare/v0.1.6...v0.1.7) - 2020.05.12


<a name="v0.1.6"></a>
## [v0.1.6](https://github.com/hpcflow/hpcflow-new/compare/v0.1.5...v0.1.6) - 2020.05.11


<a name="v0.1.5"></a>
## [v0.1.5](https://github.com/hpcflow/hpcflow-new/compare/v0.1.4...v0.1.5) - 2020.05.07


<a name="v0.1.4"></a>
## [v0.1.4](https://github.com/hpcflow/hpcflow-new/compare/v0.1.3...v0.1.4) - 2020.05.07


<a name="v0.1.3"></a>
## [v0.1.3](https://github.com/hpcflow/hpcflow-new/compare/v0.1.2...v0.1.3) - 2020.05.07


<a name="v0.1.2"></a>
## [v0.1.2](https://github.com/hpcflow/hpcflow-new/compare/v0.1.1...v0.1.2) - 2020.05.06


<a name="v0.1.1"></a>
## v0.1.1 - 2019.06.14

