#!/usr/bin/env bash
# Linux host bridge: Python and workflow metadata run in the container; scheduler
# submission and command files run on the host. Requires Bash 4.4+, jq and flock.
#
# Reading guide
# -------------
# This file is a template: "install --shell bash" replaces the __APP_... tokens
# with the application's name and default image before exporting it.
#
# The main flow is:
#   1. Read wrapper options and establish host/container directory mappings.
#   2. Define helpers for container calls, JSON records and host command execution.
#   3. Dispatch recovery, notebook, internal execute-run or ordinary CLI commands.
#   4. For go/submit: prepare a plan, validate ALL jobs, submit them on the host,
#      then ask the container to record each returned scheduler job ID.
#
# There are three different JSON documents worth distinguishing:
#   plan     - instructions prepared by the container; no host job launched yet;
#   context  - settings copied beside the workflow for queued jobs to reuse;
#   journal  - durable evidence of a host launch, kept if its outcome is uncertain.
# A journal is not a temporary file and must not be deleted blindly after errors.
#
# Bash notation used below
# ------------------------
# "$@" / "${array[@]}"  preserve each argument as a separate word, including
#                      spaces and empty strings. Do not replace these with $*.
# $(command)           captures a command's stdout (trailing newlines removed).
# [[ condition ]]      tests strings/files; (( expression )) tests arithmetic.
# command || die ...   runs die only if command returns a nonzero exit status.
# ${name:-default}     uses a default if name is unset or empty.
# ${name#prefix}       removes a matching prefix; ${name//old/new} replaces text.
# > file / < file      redirect stdout / stdin; >&2 sends messages to stderr.
# < <(command)         feeds command output into a loop without piping the loop
#                      into a subshell, so changes to its variables survive.
# <<< "$json"          supplies a string (plus newline) as a command's stdin.
#
# jq is a JSON processor, not a shell evaluator. Its "." means the current JSON
# value, "|" passes a value to the next filter, and ".field" selects a field.
# -e makes false/null results fail; -r emits unquoted strings; -c emits compact
# JSON; -j emits strings without extra newlines; -n constructs JSON without input.
# --arg passes a string safely; --argjson passes an already encoded JSON value.
# Shell and jq both use "$name", but single-quoted filters keep jq's variables
# untouched by Bash. Never use eval to turn a plan into a shell command.

# Strict mode: -e exits on unhandled failures (not failures explicitly tested by
# if/&&/||); -E inherits the ERR trap in functions/subshells; -u rejects unset
# variables; pipefail reports failures within pipelines, not just the last command.
# umask 077 makes newly created records private to the current user.
set -Eeuo pipefail
umask 077

# Scalars hold individual values; the two () assignments initialise Bash arrays.
# Wrapper options are kept separate from arguments forwarded to the application.
app='__APP_PACKAGE__'
caps='__APP_CAPS__'
image='__APP_NAME_CONTAINER_IMAGE__'
# These exported defaults can still be overridden with --image and --runtime.
runtime='__APP_RUNTIME__'
machine=$(hostname)
root=$(pwd -P)
config=''
context=''
resume=''
tty=auto
port=8888
config_args=()
args=()

# die MESSAGE...: print an error to stderr and terminate the wrapper.
die() { printf '%s\n' "$*" >&2; exit 1; }

# json_array WORD...: emit a JSON array on stdout, preserving argument boundaries.
# NUL (\0) separates words because spaces/newlines may occur inside an argument.
# Bash strings cannot contain NUL, so it is safe as a delimiter here.
json_array() {
    if (($#)); then printf '%s\0' "$@" | jq -Rs 'split("\u0000")[:-1]'
    else printf '[]'; fi
}

# usage: print wrapper help. The unquoted EOF marker permits $app substitution
# within this "here-document"; it does not execute the example commands.
usage() {
    cat <<EOF
Usage: $app.sh [wrapper options] -- COMMAND [ARGUMENTS...]
  --runtime docker|apptainer|singularity   Default: $runtime
  --image IMAGE                           Default: $image (Docker image or SIF file)
  --machine NAME                          Default: host hostname
  --config-dir DIRECTORY                  Shared container configuration directory
  --config-arg WORD                        Repeat for global application options
  --tty | --no-tty                         Automatic for interactive go/submit
  --notebook-port PORT                     Default: 8888
  --resume-result JOURNAL                  Acknowledge only; never relaunch
  --context FILE                          Internal scheduled-job context
  --help                                  Show this wrapper help

Requires Linux, Bash 4.4+, jq, flock and the selected container runtime.
Supports Slurm/SGE submission and queued-job command execution. Direct jobs,
host monitoring, wait and cancellation are not supported in this first version.
Examples:
  ./$app.sh -- go workflow.yaml
  ./$app.sh --runtime apptainer --image $app.sif -- go workflow.yaml
  ./$app.sh --runtime singularity --image $app.sif -- notebook
EOF
}

# Parse positional parameters: $# is their count, $1/$2 are the next words.
# shift removes consumed words; shift 2 consumes an option and its value.
# case ... ;; selects one branch. "--" ends wrapper options, and everything
# after it is forwarded literally to hpcflow/MatFlow.
while (($#)); do
    case $1 in
        --help) usage; exit 0 ;;
        --tty) tty=yes; shift ;;
        --no-tty) tty=no; shift ;;
        --) shift; args=("$@"); break ;;
        --runtime|--image|--machine|--config-dir|--config-arg|--context|--resume-result|--notebook-port)
            (($# >= 2)) || die "Missing value for $1."
            case $1 in
                --runtime) runtime=$2 ;;
                --image) image=$2 ;;
                --machine) machine=$2 ;;
                --config-dir) config=$2 ;;
                --config-arg) config_args+=("$2") ;;
                --context) context=$2 ;;
                --resume-result) resume=$2 ;;
                --notebook-port) port=$2 ;;
            esac
            shift 2 ;;
        *) args=("$@"); break ;;
    esac
done

# Check requirements before attempting any host launch. command -v finds an
# executable; >/dev/null hides its path while preserving its success/failure.
[[ $(uname -s) == Linux ]] || die 'This wrapper currently supports Linux only.'
((BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4))) ||
    die 'Bash 4.4 or newer is required.'
for tool in jq flock realpath sha256sum; do
    command -v "$tool" >/dev/null || die "Required host tool not found: $tool"
done

# All temporary protocol files are private and removed on normal exit/signals.
# Durable launch journals are deliberately NOT included in this cleanup.
temporary=$(mktemp -d)
startup_pid=''
# cleanup: EXIT-trap handler. $? is the previous exit status; preserve it while
# removing disposable files. Kill only the spinner PID started by this wrapper.
# "|| true" tolerates the spinner already having exited, not application errors.
cleanup() {
    local status=$?
    if [[ -n $startup_pid ]]; then
        kill "$startup_pid" 2>/dev/null || true
        wait "$startup_pid" 2>/dev/null || true
    fi
    [[ -z ${started:-} ]] || rm -f -- "$started"
    [[ -z ${plan_file:-} ]] || rm -f -- "$plan_file"
    rm -f -- "$temporary"/*
    rmdir -- "$temporary"
    return "$status"
}
# A trap arranges for a handler to run on exit or a signal. Ctrl+C sends INT;
# TERM requests termination. These conventional statuses identify interrupted runs.
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'printf "Host bridge failed at line %s (exit %s).\n" "$LINENO" "$?" >&2' ERR

# Queued jobs use --context to restore the submitter's mount/image/configuration,
# rather than deriving settings from a compute node's home/current directory.
# Validate types before extracting values from the JSON file.
if [[ -n $context ]]; then
    jq -e ' .schema_version == 1 and
        ([.mount_root,.image,.runtime,.machine,.config_directory,.workflow_id] |
         all(type == "string" and length > 0 and (contains("\u0000") | not))) and
        (.config_args | type == "array" and all(type == "string" and (contains("\u0000") | not)))' "$context" >/dev/null ||
        die 'Invalid scheduled-job context.'
    root=$(jq -r .mount_root "$context")
    image=$(jq -r .image "$context")
    runtime=$(jq -r .runtime "$context")
    machine=$(jq -r .machine "$context")
    config=$(jq -r .config_directory "$context")
    # mapfile reads an array. With -d '', NUL terminates each word, so embedded
    # newlines/spaces do not split an argument; -t removes the delimiter.
    mapfile -d '' -t config_args < <(jq -j '.config_args[] + "\u0000"' "$context")
fi
case $runtime in docker|apptainer|singularity) ;; *) die "Unsupported runtime: $runtime" ;; esac
command -v "$runtime" >/dev/null || die "Container runtime not found: $runtime"
[[ -n $image && $image != __* && -n $machine ]] || die 'Image and machine must not be empty.'
root=$(realpath -e -- "$root")
if [[ -z $config ]]; then
    config_var="${caps}_CONFIG_DIR"
    # ${!config_var} is indirect expansion: read the environment variable whose
    # NAME is stored in config_var, e.g. HPCFLOW_CONFIG_DIR.
    config="${!config_var:-$HOME/.$app}-container"
fi
mkdir -p -- "$config"
config=$(realpath -e -- "$config")
container_config="/config/.$app-container"
if [[ $runtime != docker && $image != docker://* && $image != oras://* ]]; then
    image=$(realpath -e -- "$image")
fi
# mount_id HOST_ROOT: emit a stable SHA-256 identifier, not a security credential.
# The same host root then has the same /host-workflows/ID alias across invocations.
mount_id() { printf '%s' "$1" | sha256sum | cut -d ' ' -f 1; }
# Parallel arrays: mount_keys[i] is the container path corresponding to the
# host path mount_values[i]. /work maps the current root; /config holds storage.
mount_keys=('/work' "$container_config")
mount_values=("$root" "$config")
# Restore registered workflow roots so references/local IDs can resolve when
# invoked from a different directory. A missing glob match is skipped explicitly.
for file in "$config"/host-mounts/*.json; do
    [[ -e $file ]] || continue
    jq -e '.schema_version == 1 and (.mount_root | type == "string" and startswith("/") and
        (contains("\u0000") | not))' "$file" >/dev/null ||
        die "Invalid workflow mount record: $file"
    saved_root=$(jq -r .mount_root "$file")
    [[ $(basename "$file" .json) == "$(mount_id "$saved_root")" ]] ||
        die "Workflow mount identity mismatch: $file"
    mount_keys+=("/host-workflows/$(mount_id "$saved_root")")
    mount_values+=("$saved_root")
done
# Resolve both directions without eval, shell expansion or substring path escapes.
# host_path CONTAINER_PATH: emit an absolute host path or fail if it escapes a
# recognised mount (including via .. or a symlink). realpath -m resolves paths
# even if their final component does not exist yet.
host_path() {
    local path=$1 i relative resolved base
    # ${!array[@]} expands array indices, rather than their stored values.
    for i in "${!mount_keys[@]}"; do
        base=${mount_keys[i]}
        if [[ $path == "$base" || $path == "$base/"* ]]; then
            relative=${path#"$base"}
            [[ /$relative/ != */../* && $relative != *\\* ]] || die "Invalid container path: $path"
            resolved=$(realpath -m -- "${mount_values[i]}$relative")
            [[ $resolved == "${mount_values[i]}" || $resolved == "${mount_values[i]}/"* ]] ||
                die "Path escapes host mount: $path"
            printf '%s' "$resolved"
            return
        fi
    done
    die "Path is outside recognised container mounts: $path"
}
# container_path WORD: map a recognised absolute host path to its container alias;
# leave other argument words unchanged. Choose the longest matching host root
# when nested mounts exist (e.g. configuration inside the workflow root).
container_path() {
    local path=$1 i best=-1
    for i in "${!mount_values[@]}"; do
        if [[ $path == "${mount_values[i]}" || $path == "${mount_values[i]}/"* ]]; then
            if ((best < 0)) || ((${#mount_values[i]} > ${#mount_values[best]})); then best=$i; fi
        fi
    done
    if ((best >= 0)); then
        printf '%s%s' "${mount_keys[best]}" "${path#"${mount_values[best]}"}"
    else
        printf '%s' "$path"
    fi
}
# map_host_value WORD: translate container paths in a run's argv/environment;
# preserve non-path values instead of treating every argument as a file.
map_host_value() {
    case $1 in /work|/work/*|/host-workflows/*|/config/*) host_path "$1" ;; *) printf '%s' "$1" ;; esac
}

# Atomic replacement plus fsync keeps a launch intent or its complete result on
# disk. flock serialises launches/recovery for each workflow.
# save_record DESTINATION JSON: write a unique sibling, flush it, rename over the
# destination, then flush the containing filesystem. Readers see a complete old
# or new record rather than a half-written JSON document.
save_record() {
    local destination=$1 json=$2 pending
    pending=$(mktemp "${destination}.XXXXXX")
    printf '%s\n' "$json" > "$pending"
    sync -f "$pending"
    mv -f -- "$pending" "$destination"
    sync -f "$(dirname "$destination")"
}

# startup_indicator: background helper reading the shared "started" path.
# "-t 2" checks whether stderr is a terminal. \r returns to the beginning of
# the current line; \033[2K clears it. Redirected logs get one plain message.
startup_indicator() {
    local frames=('|' '/' '-' '\') n=0
    if [[ -t 2 ]]; then
        while [[ ! -f $started ]]; do
            printf '\r%s Starting container...' "${frames[n % 4]}" >&2
            n=$((n + 1))
            sleep 0.1
        done
        printf '\r\033[2K' >&2
    else
        printf 'Starting container...\n' >&2
    fi
}

# capture: JSON stdout to the requested file; display: inherit terminal streams.
# Unlike Docker, Apptainer shares host networking and does not require -t/-i.
# invoke_container MODE OUTPUT_FILE ARG...: assemble literal runtime argv and
# run it. Returns the runtime's exit status; stdin is inherited for JSON input.
# Globals read: runtime/image, mounts, config_args, tty, port, submission_plan.
# Globals updated: started/plan_file (temporary exchange paths), startup_pid.
# notebook mode replaces the app with Jupyter. submission_plan=yes exchanges
# JSON through a shared file so progress/TTY output cannot corrupt the plan.
invoke_container() {
    local mode=$1 output=$2; shift 2
    local use_tty=no code i word name
    # local confines helper variables to this function; -a declares arrays.
    local -a command=() mapped=() environment=()
    if [[ $mode == display && ( $tty == yes || ( $tty == auto && -t 1 ) ) ]]; then use_tty=yes; fi
    started=$(mktemp "$config/.host-started-XXXXXX")
    # Reserve a unique name, then remove the file: the entrypoint creates it
    # when the container starts. Leaving mktemp's empty file would signal too soon.
    rm -- "$started"
    # These are NAME=value words for the container, not exports to the host.
    # Host identity selects Linux jobscripts even though preparation runs inside.
    environment=(
        "${caps}_CONFIG_DIR=$container_config"
        "${caps}_CONTAINER=$image"
        "${caps}_CONTAINER_HOST_OS=posix"
        "${caps}_CONTAINER_HOST_HOSTNAME=$(hostname)"
        "${caps}_CONTAINER_HOST_CPU_ARCH=$(uname -m)"
        "${caps}_CONTAINER_WORK_MOUNT=/host-workflows/$(mount_id "$root")"
        "${caps}_CONTAINER_HOST_CONFIG_DIR=$config"
        "${caps}_CONTAINER_HOST_WORK_DIR=$root"
        "XDG_CACHE_HOME=$container_config/cache" "XDG_DATA_HOME=$container_config/data"
        "HPCFLOW_CONTAINER_STARTED_FILE=$container_config/$(basename "$started")"
        "PYTHONNOUSERSITE=1"
    )
    # Forward exported jobscript state, not container identity/configuration or
    # an executor port belonging to a different process. Map shared host paths.
    while IFS= read -r name; do
        [[ $name == "${caps}_"* ]] || continue
        case $name in
            "${caps}_CONFIG_DIR"|*_CONTAINER*|"${caps}_RUN_PORT") continue ;;
        esac
        environment+=("$name=$(container_path "${!name}")")
    done < <(compgen -e)
    if [[ $use_tty == yes ]]; then
        environment+=('TTY_INTERACTIVE=1' 'TERM=xterm-256color')
    else
        environment+=('TTY_INTERACTIVE=0')
    fi
    if [[ $mode == capture ]]; then
        environment+=('NO_COLOR=1' 'FORCE_COLOR=')
    fi
    if [[ ${submission_plan:-no} == yes ]]; then
        plan_file=$(mktemp "$config/.host-plan-XXXXXX")
        rm -- "$plan_file"
        environment+=("${caps}_CONTAINER_PLAN_FILE=$container_config/$(basename "$plan_file")")
    fi
    if [[ $mode == notebook ]]; then
        mapped=('python' '-m' 'jupyterlab' '--ServerApp.ip=127.0.0.1'
            "--ServerApp.port=$port" '--ServerApp.port_retries=0' '--ServerApp.open_browser=False'
            '--ServerApp.allow_root=True' '--ServerApp.root_dir=/work')
    else
        mapped=('--with-config' 'machine' "$machine")
        for word in "${config_args[@]}" "$@"; do mapped+=("$(container_path "$word")"); done
    fi
    if [[ $runtime == docker ]]; then
        # -i forwards stdin; -t allocates a pseudo-terminal only for display.
        # --rm removes the stopped container, not the bind-mounted host files.
        command=(docker run --rm -i)
        [[ $use_tty != yes ]] || command+=(-t)
        if [[ $mode == notebook ]]; then
            command+=(--publish "127.0.0.1:$port:$port" --entrypoint hpcflow-container-entrypoint)
            mapped[3]='--ServerApp.ip=0.0.0.0'
        fi
        for i in "${!mount_keys[@]}"; do
            [[ -d ${mount_values[i]} ]] || continue
            [[ ${mount_values[i]} != *,* ]] || die 'Docker mount paths must not contain commas.'
            command+=(--mount "type=bind,source=${mount_values[i]},target=${mount_keys[i]}")
        done
        command+=(--workdir /work)
    else
        # run invokes the SIF runscript; exec chooses our notebook entrypoint.
        # --cleanenv/--contain limit inherited environment/implicit mounts;
        # --no-eval avoids another shell evaluation of argv/environment values.
        command=("$runtime")
        if [[ $mode == notebook ]]; then command+=(exec); else command+=(run); fi
        command+=(--cleanenv --contain --no-eval --pwd /work)
        for i in "${!mount_keys[@]}"; do
            [[ -d ${mount_values[i]} ]] || continue
            [[ ${mount_values[i]} != *[:,]* ]] || die 'Apptainer bind paths must not contain colons or commas.'
            command+=(--bind "${mount_values[i]}:${mount_keys[i]}")
        done
    fi
    for word in "${environment[@]}"; do
        # --env values containing commas are parsed differently by Apptainer.
        if [[ $runtime != docker && $word == *,* ]]; then die 'Container environment values must not contain commas.'; fi
        command+=(--env "$word")
    done
    command+=("$image")
    if [[ $runtime != docker && $mode == notebook ]]; then command+=(hpcflow-container-entrypoint); fi
    command+=("${mapped[@]}")
    # "&" starts only the indicator in the background. $! is that process's PID.
    # The runtime stays foreground so command output and input remain interactive.
    startup_indicator &
    startup_pid=$!
    code=0
    if [[ $mode == capture ]]; then
        # Executing an array preserves argv exactly; no shell-string construction.
        # Capture a failure explicitly so we can finish cleanup and return its code.
        "${command[@]}" > "$output" || code=$?
    else
        "${command[@]}" || code=$?
    fi
    # Also release the indicator after a failed/legacy entrypoint with no signal.
    : > "$started"
    wait "$startup_pid" || true
    startup_pid=''
    rm -f -- "$started"
    started=''
    if [[ ${submission_plan:-no} == yes && $code == 0 ]]; then
        [[ -f $plan_file ]] || die 'Container did not write a host submission plan.'
        cp -- "$plan_file" "$output"
    fi
    [[ -z ${plan_file:-} ]] || rm -f -- "$plan_file"
    plan_file=''
    return "$code"
}

# confirm_result ENVELOPE_JSON: send only its result to the container on stdin.
# The "-" result-file argument tells hpcflow to read stdin, not a named file.
# After a valid acknowledgement, register the host root for later mount lookup.
# Failure leaves the caller's durable journal available for acknowledgement retry.
confirm_result() {
    local envelope=$1 response="$temporary/ack.json" result_file="$temporary/result.json"
    jq '.result' <<< "$envelope" > "$result_file"
    invoke_container capture "$response" internal workflow "$(jq -r .workflow_path <<< "$envelope")" \
        record-host-submission - < "$result_file" || die 'Acknowledgement failed; retain the journal and use --resume-result.'
    jq -e '.recorded | type == "boolean"' "$response" >/dev/null || die 'Invalid acknowledgement response.'
    mkdir -p -- "$config/host-mounts"
    save_record "$config/host-mounts/$(mount_id "$root").json" \
        "$(jq -n --arg root "$root" '{schema_version:1,mount_root:$root}')"
}

# Queued jobs call this bridge too: only their command files execute on the host.
# Direct *job scheduling* is deliberately deferred, not command-file execution.
# execute_host_run: use global args (internal workflow ... execute-run indices).
# Prepare a run plan in the container, map its command/environment to host paths,
# execute it once, then report its exit code back to the container.
# A null plan means there is no host command to execute. A command's nonzero exit
# is reported to hpcflow; a failed protocol completion instead leaves a journal.
execute_host_run() {
    local plan="$temporary/run.json" workflow_path working_directory journal code=0 key value
    local -a command=() environment=() complete=("${args[@]}")
    invoke_container capture "$plan" "${args[@]}" --containerised || die 'Run preparation failed.'
    [[ $(jq -r type "$plan") != null ]] || return 0
    jq -e --arg caps "$caps" '
        .schema_version == 1 and (.workflow_id | type == "string") and
        (.working_directory | type == "string") and
        (.command | type == "array" and length > 0 and all(type == "string" and (contains("\u0000") | not))) and
        (.environment | type == "object" and (to_entries | all(
            (.key | test("^[A-Za-z_][A-Za-z_0-9]*$")) and
            (.key | contains("_CONTAINER") | not) and .key != ($caps + "_RUN_PORT") and
            (.value | type == "string" and (contains("\u0000") | not)))))' "$plan" >/dev/null ||
        die 'Invalid host run plan.'
    if [[ -n $context ]]; then
        [[ $(jq -r .workflow_id "$plan") == "$(jq -r .workflow_id "$context")" ]] ||
            die 'Run belongs to a different workflow.'
    fi
    workflow_path=$(host_path "$(container_path "${args[2]}")")
    working_directory=$(host_path "$(jq -r .working_directory "$plan")")
    [[ -d $working_directory ]] || die 'Host run working directory does not exist.'
    journal="$workflow_path/.$app-run-${args[4]}-${args[5]}-${args[6]}-${args[7]}-${args[8]}.json"
    # Descriptor 8 keeps the lock file open; flock -n requests an exclusive lock
    # without waiting. The lock protects simultaneous attempts; the journal below
    # protects later retries after an uncertain outcome. Lock-file existence alone
    # does NOT mean it is locked. See the submission-lock note below for Lustre.
    exec 8>"$journal.lock"
    flock -n 8 || die 'Another process is executing this run.'
    [[ ! -e $journal ]] || die "Unresolved host run journal: $journal. Reconcile before retrying."
    # IFS= disables whitespace splitting; read -r preserves backslashes; -d ''
    # reads NUL-delimited words. Process substitution keeps this loop in our shell.
    while IFS= read -r -d '' value; do command+=("$(map_host_value "$value")"); done \
        < <(jq -j '.command[] + "\u0000"' "$plan")
    while IFS= read -r -d '' key && IFS= read -r -d '' value; do
        environment+=("$key=$(map_host_value "$value")")
    done < <(jq -j '.environment | to_entries[] | .key + "\u0000" + .value + "\u0000"' "$plan")
    environment+=("${caps}_CONFIG_DIR=$config")
    [[ -n ${command[0]} ]] || die 'Missing host command executable.'
    save_record "$journal" "$(jq -n --argjson args "$(printf '%s\0' "${args[@]}" | jq -Rs 'split("\u0000")[:-1]')" \
        '{state:"launching",run_args:$args}')"
    # (...) runs in a subshell so cd/environment changes cannot leak into the
    # wrapper. env applies overrides only to the command. 8>&- closes its copy
    # of our lock descriptor; the parent retains the lock during execution.
    (cd -- "$working_directory" && env "${environment[@]}" "${command[@]}" 8>&-) || code=$?
    complete[3]=complete-host-run
    save_record "$journal" "$(jq -n --argjson code "$code" '{state:"executed",exit_code:$code}')"
    invoke_container capture "$temporary/completed.json" "${complete[@]}" -- "$code" ||
        die "Run completion failed. Reconcile $journal; do not re-execute."
    jq -e '.completed == true' "$temporary/completed.json" >/dev/null || die 'Invalid run completion response.'
    rm -- "$journal"
}

# Main dispatch begins here. Recovery deliberately skips preparation/submission:
# only a "submitted" journal can be acknowledged without launching another job.
if [[ -n $resume ]]; then
    ((${#args[@]} == 0)) || die 'Recovery cannot be combined with application commands.'
    exec 9>"$resume.lock"
    flock -n 9 || die 'Another process is recovering this submission.'
    envelope=$(cat -- "$resume")
    jq -e --arg root "$root" --arg image "$image" --arg runtime "$runtime" --arg config "$config" '
        .state == "submitted" and .mount_root == $root and .image == $image and .runtime == $runtime and
        .config_directory == $config and (.config_args | type == "array" and all(type == "string")) and
        (.result.submit_machine | type == "string" and length > 0)' \
        <<< "$envelope" >/dev/null || die 'Uncertain launch outcome or mismatched recovery settings; inspect the journal and scheduler.'
    machine=$(jq -r .result.submit_machine <<< "$envelope")
    mapfile -d '' -t config_args < <(jq -j '.config_args[] + "\u0000"' <<< "$envelope")
    host_path "$(jq -r .workflow_path <<< "$envelope")" >/dev/null
    confirm_result "$envelope"
    rm -- "$resume"
    printf 'Host submission acknowledged; no job was launched.\n'
    exit 0
fi
((${#args[@]})) || die 'Supply an application command or --resume-result.'

# Generated jobscripts prepend global options. Keep these separate so command
# dispatch cannot accidentally bypass host execution or submission.
# Array slices ${args[@]:offset:count} select words; omitting count takes the rest.
while [[ ${args[0]} == --* && ${#args[@]} -gt 1 ]]; do
    case ${args[0]} in
        --with-config) count=3 ;;
        --config-dir|--config-key|--std-stream|--timeit-file) count=2 ;;
        --timeit) count=1 ;;
        *) break ;;
    esac
    ((${#args[@]} > count)) || die 'Missing command after global option.'
    config_args+=("${args[@]:0:count}")
    args=("${args[@]:count}")
done
if [[ ${args[0]} == notebook ]]; then
    ((${#args[@]} == 1 && ${#config_args[@]} == 0)) || die 'Use notebook without extra server arguments.'
    [[ $port =~ ^[0-9]{1,5}$ ]] && ((10#$port >= 1 && 10#$port <= 65535)) || die 'Notebook port must be 1..65535.'
    # 10# forces decimal interpretation (a leading zero otherwise means octal).
    port=$((10#$port))
    printf 'Starting JupyterLab on localhost:%s; use the token URL. Remote access requires an SSH tunnel.\n' "$port" >&2
    invoke_container notebook '' || exit $?
    exit 0
fi
if [[ ${args[0]} == internal && ${args[1]:-} == workflow && ${args[3]:-} == execute-run ]]; then
    ((${#args[@]} >= 9)) || die 'Incomplete execute-run invocation.'
    for word in "${args[@]:4:5}"; do
        [[ $word =~ ^[0-9]+$ ]] || die 'Run indices must be nonnegative integers.'
    done
    execute_host_run
    exit 0
fi
if [[ ${args[0]} == submission && ${args[1]:-} == get-known ]]; then
    die 'Host monitoring is not supported by this Bash wrapper yet.'
fi
if [[ ${args[0]} == show && " ${args[*]} " != *' --help '* && " ${args[*]} " != *' --legend '* ]]; then
    die 'Host monitoring is not supported by this Bash wrapper yet.'
fi
# Normalise workflow group reference options accepted before or after the reference.
# Click expects "workflow --ref-type id 0 submit"; accept the natural alternative
# "workflow 0 --ref-type id submit" too, then rebuild args in the required order.
if [[ ${args[0]} == workflow ]]; then
    ref_options=()
    index=1
    while [[ ${args[index]:-} == --ref-type || ${args[index]:-} == -r || ${args[index]:-} == --ref-type=* ]]; do
        case ${args[index]} in
            --ref-type|-r) [[ -n ${args[index+1]:-} ]] || die 'Missing workflow reference type.'
                ref_options+=("${args[index]}" "${args[index+1]}"); index=$((index + 2)) ;;
            *) ref_options+=("${args[index]}"); index=$((index + 1)) ;;
        esac
    done
    reference=${args[index]:-}
    [[ -n $reference ]] || die 'Missing workflow reference.'
    index=$((index + 1))
    while [[ ${args[index]:-} == --ref-type || ${args[index]:-} == -r || ${args[index]:-} == --ref-type=* ]]; do
        case ${args[index]} in
            --ref-type|-r) [[ -n ${args[index+1]:-} ]] || die 'Missing workflow reference type.'
                ref_options+=("${args[index]}" "${args[index+1]}"); index=$((index + 2)) ;;
            *) ref_options+=("${args[index]}"); index=$((index + 1)) ;;
        esac
    done
    args=(workflow "${ref_options[@]}" "$reference" "${args[@]:index}")
    case ${args[index]:-} in
        abort-run) die 'Aborting container host runs is not supported yet.' ;;
        wait|cancel|show-all-status)
            die 'Host monitoring and cancellation are not supported by this Bash wrapper yet.' ;;
    esac
fi
submission=no
if [[ ${args[0]} == go || ( ${args[0]} == demo-workflow && ${args[1]:-} == go ) ]]; then submission=yes; fi
if [[ ${args[0]} == workflow && ${args[index]:-} == submit ]]; then submission=yes; fi
for word in "${args[@]}"; do
    [[ $word != --help ]] || submission=no
    [[ $word != --json ]] || tty=no
done
if [[ $submission == no ]]; then invoke_container display '' "${args[@]}" || exit $?; exit 0; fi
for word in "${args[@]}"; do
    [[ $word != --wait && $word != --cancel ]] || die 'Containerised submission cannot wait or cancel.'
done
# All remaining go/submit invocations first prepare, never submit in-container.
plan="$temporary/plan.json"
submission_plan=yes
invoke_container display "$plan" "${args[@]}" --containerised || die 'Workflow preparation failed.'
submission_plan=no

# Validate the entire plan before launching anything, including topological
# dependency ordering, numeric references, paths and every argv string.
# This single-quoted block is jq code, NOT Bash. The helper predicates check
# nonnegative integer indices, NUL-free strings and safe relative paths.
# all(...) checks every entry; unique detects duplicate jobscript identities.
jq -e --arg caps "$caps" '
    def integer: type == "number" and . >= 0 and floor == .;
    def string: type == "string" and (contains("\u0000") | not);
    def relative: string and length > 0 and (startswith("/") | not) and
        (contains("\\") | not) and (split("/") | index("..") | not);
    .schema_version == 1 and (.workflow_id | string and length > 0) and
    (.workflow_path | string) and (.jobscripts | type == "array") and
    ([.jobscripts[] | [ .submission_index, .jobscript_index ]] |
        length == (unique | length)) and
    (all(.jobscripts[]; . as $job |
            ($job.submission_index | integer) and ($job.jobscript_index | integer) and
            ($job.scheduler == "slurm" or $job.scheduler == "sge") and $job.shell == "bash" and
            ($job.is_array | type == "boolean") and ($job.path | relative) and
            ($job.submit_command | type == "array" and length > 0 and all(string)) and
            ($job.submit_command[0] | length > 0) and
            ($job.submit_command | any(contains("__" + $caps + "_JOBSCRIPT_PATH__"))) and
            ($job.dependencies | type == "array") and
            (all($job.dependencies[];
                . as $dep |
                (.submission_index | integer) and (.jobscript_index | integer) and
                .placeholder == ("__" + $caps + "_JOB_" + (.submission_index|tostring) + "_" + (.jobscript_index|tostring) + "__") and
                (.reference == null or (.reference | type == "string" and test("^[0-9]+$")))))))
    ' "$plan" >/dev/null || die 'Invalid submission plan; only Slurm/SGE Bash jobs are supported.'
# Dependency checks use the same ordered prefix rather than trusting placeholder text.
# seen is a JSON array of "submission:jobscript" keys already validated. Each
# unresolved dependency must refer to an earlier job, not itself or a later job.
# Temporarily remove declared tokens in jq to detect any undeclared placeholders.
seen='[]'
while IFS= read -r job; do
    jq -e --argjson seen "$seen" --arg caps "$caps" '
        all(.dependencies[]; . as $dep | .reference != null or
            (($seen | index(($dep.submission_index|tostring) + ":" + ($dep.jobscript_index|tostring))) != null)) and
        (. as $job | all(.submit_command[];
            reduce (["__" + $caps + "_JOBSCRIPT_PATH__"] + [$job.dependencies[].placeholder])[] as $token
                (.; split($token) | join("")) |
            (test("__" + $caps + "_(JOBSCRIPT_PATH|JOB_[0-9]+_[0-9]+)__") | not)))' \
        <<< "$job" >/dev/null || die 'Unresolved or out-of-order dependency.'
    seen=$(jq --argjson seen "$seen" '$seen + [(.submission_index|tostring) + ":" + (.jobscript_index|tostring)]' <<< "$job")
    host_script=$(host_path "$(jq -r .workflow_path "$plan")/$(jq -r .path <<< "$job")")
    [[ -f $host_script ]] || die "Missing host jobscript: $host_script"
done < <(jq -c '.jobscripts[]' "$plan")
workflow_host=$(host_path "$(jq -r .workflow_path "$plan")")
[[ -d $workflow_host ]] || die 'Host workflow directory does not exist.'
journal="$workflow_host/.hpcflow-host-submission.json"
# Descriptor 9 holds the submission/recovery lock until this wrapper exits.
# Locks are advisory: other programs must cooperate to be protected. On Lustre
# their scope depends on mount options: localflock protects one client, flock
# can coordinate clients, and noflock is unsupported. Our usage assumes the same
# workflow/run is not launched concurrently from different nodes.
# Keep the .lock file: unlinking it while another process holds its inode can
# let a new invocation lock a different inode and bypass mutual exclusion.
exec 9>"$journal.lock"
flock -n 9 || die 'Another process is submitting this workflow.'
[[ ! -e $journal ]] || die "Unresolved launch journal: $journal. Recover before submitting again."
wrapper_path="$workflow_host/.$app.sh"
# Copy the wrapper and a reusable context beside the workflow. Generated queued
# jobs find these on shared storage without requiring scheduler environment export.
if [[ $(realpath -e -- "${BASH_SOURCE[0]}") != "$wrapper_path" ]]; then
    cp -- "$(realpath -e -- "${BASH_SOURCE[0]}")" "$wrapper_path"
fi
context_path="$workflow_host/.$app-container-context.json"
save_record "$context_path" "$(jq -n --arg root "$root" --arg image "$image" --arg runtime "$runtime" \
    --arg machine "$machine" --arg config "$config" --arg id "$(jq -r .workflow_id "$plan")" \
    --argjson config_args "$(json_array "${config_args[@]}")" \
    '{schema_version:1,mount_root:$root,image:$image,runtime:$runtime,machine:$machine,config_directory:$config,workflow_id:$id,config_args:$config_args}')"
# -A declares an associative array (dictionary), mapping "submission:jobscript"
# to the actual scheduler ID returned by an earlier successful submission.
declare -A refs=()
while IFS= read -r job; do
    host_script=$(host_path "$(jq -r .workflow_path "$plan")/$(jq -r .path <<< "$job")")
    command=()
    while IFS= read -r -d '' word; do
        # Parameter substitution replaces placeholders as text within ONE argv
        # word. Even a word containing "$(command)" remains literal, not executed.
        word=${word//__APP_NAME_JOBSCRIPT_PATH__/"$host_script"}
        while IFS= read -r dep; do
            key=$(jq -r '(.submission_index|tostring) + ":" + (.jobscript_index|tostring)' <<< "$dep")
            ref=$(jq -r '.reference // empty' <<< "$dep")
            [[ -n $ref ]] || ref=${refs[$key]}
            token=$(jq -r .placeholder <<< "$dep")
            word=${word//"$token"/"$ref"}
        done < <(jq -c '.dependencies[]' <<< "$job")
        [[ ! $word =~ __${caps}_(JOBSCRIPT_PATH|JOB_[0-9]+_[0-9]+)__ ]] || die 'Unresolved command placeholder.'
        command+=("$word")
    done < <(jq -j '.submit_command[] + "\u0000"' <<< "$job")
    # result describes one launch; envelope adds recovery settings and its state.
    # Persist "launching" BEFORE calling the scheduler: a crash afterwards must
    # not allow a retry to assume that nothing was submitted.
    result=$(jq -n --argjson job "$job" --arg id "$(jq -r .workflow_id "$plan")" \
        --arg machine "$machine" --arg host "$(hostname)" --arg time "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" \
        --argjson command "$(printf '%s\0' "${command[@]}" | jq -Rs 'split("\u0000")[:-1]')" \
        '{schema_version:1,workflow_id:$id,submission_index:$job.submission_index,jobscript_index:$job.jobscript_index,
          submit_command:$command,submit_time:$time,submit_hostname:$host,submit_machine:$machine}')
    envelope=$(jq -n --arg root "$root" --arg image "$image" --arg runtime "$runtime" \
        --arg workflow "$(jq -r .workflow_path "$plan")" --argjson result "$result" \
        --arg config "$config" --argjson config_args "$(json_array "${config_args[@]}")" \
        '{state:"launching",mount_root:$root,image:$image,runtime:$runtime,workflow_path:$workflow,
          config_directory:$config,config_args:$config_args,result:$result}')
    save_record "$journal" "$envelope"
    code=0
    # Submit from the host workflow directory, capturing both streams separately.
    # Close descriptor 9 in the child so it cannot keep our lock after we exit.
    (cd -- "$workflow_host" && export "${caps}_CONTAINER_WRAPPER=$wrapper_path" \
        "${caps}_CONTAINER_CONTEXT=$context_path" && "${command[@]}" 9>&-) \
        > "$temporary/scheduler.stdout" 2> "$temporary/scheduler.stderr" || code=$?
    # --rawfile reads an entire stream as a JSON string, preserving its contents.
    # Save output BEFORE parsing IDs so unexpected responses can be reconciled.
    envelope=$(jq --rawfile stdout "$temporary/scheduler.stdout" --rawfile stderr "$temporary/scheduler.stderr" \
        --argjson code "$code" '. + {stdout:$stdout,stderr:$stderr,exit_code:$code}' <<< "$envelope")
    save_record "$journal" "$envelope"
    [[ $code == 0 && ! -s $temporary/scheduler.stderr ]] || die "Host submission failed; inspect $journal before retrying."
    scheduler=$(jq -r .scheduler <<< "$job")
    output=$(cat "$temporary/scheduler.stdout")
    # =~ matches a regular expression; BASH_REMATCH[1] is its first capture group.
    # Slurm --parsable returns ID[;cluster]; SGE -terse may return ID.array-range.
    if [[ $scheduler == slurm && $output =~ ^([0-9]+)(\;[^[:space:]\;]+)?$ ]]; then ref=${BASH_REMATCH[1]}
    elif [[ $scheduler == sge && $output =~ ^([0-9]+)(\.[0-9]+-[0-9]+:[0-9]+)?$ ]]; then ref=${BASH_REMATCH[1]}
    else die "Cannot parse scheduler ID; inspect $journal. Do not resubmit blindly."; fi
    # "submitted" means a scheduler ID is known; it does not mean the job finished.
    # Save that state before acknowledgement. Only remove the journal after the
    # container confirms the metadata update; otherwise --resume-result can retry it.
    envelope=$(jq --arg ref "$ref" '.state="submitted" | .result.scheduler_job_ID=$ref' <<< "$envelope")
    save_record "$journal" "$envelope"
    confirm_result "$envelope"
    rm -- "$journal"
    key=$(jq -r '(.submission_index|tostring) + ":" + (.jobscript_index|tostring)' <<< "$job")
    refs[$key]=$ref
    printf 'Submitted jobscript %s: %s\n' "$key" "$ref"
done < <(jq -c '.jobscripts[]' "$plan")
