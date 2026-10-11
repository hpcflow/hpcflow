#!/bin/sh
set -eu

# Notify the host before Python imports or application initialisation begin.
if [ -n "${HPCFLOW_CONTAINER_STARTED_FILE:-}" ]; then
    printf 'started\n' > "$HPCFLOW_CONTAINER_STARTED_FILE"
fi

exec "$@"
