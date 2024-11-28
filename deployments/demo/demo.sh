#!/usr/bin/env bash

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
# shellcheck disable=SC1090
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

set -eu

export STATE_PATH="${HOME}/playground_demo"
mkdir -p "${STATE_PATH}/playground_builder_cache"
rm -rf "${STATE_PATH}/playground_builder_filepool"
mkdir -p "${STATE_PATH}/playground_builder_filepool"

for replica in a b; do
  for shard in 0 1 2 3; do
    REPLICA="${replica}" SHARD="${shard}" \
    "$(rlocation com_github_buildbarn_bb_playground/cmd/playground_storage_shard/playground_storage_shard_/playground_storage_shard)" \
        "$(rlocation com_github_buildbarn_bb_playground/deployments/demo/playground_storage_shard.jsonnet)" &
  done
done

"$(rlocation com_github_buildbarn_bb_playground/cmd/playground_storage_frontend/playground_storage_frontend_/playground_storage_frontend)" \
    "$(rlocation com_github_buildbarn_bb_playground/deployments/demo/playground_storage_frontend.jsonnet)" &
"$(rlocation com_github_buildbarn_bb_playground/cmd/playground_builder/playground_builder_/playground_builder)" \
    "$(rlocation com_github_buildbarn_bb_playground/deployments/demo/playground_builder.jsonnet)" &
"$(rlocation com_github_buildbarn_bb_playground/cmd/playground_scheduler/playground_scheduler_/playground_scheduler)" \
    "$(rlocation com_github_buildbarn_bb_playground/deployments/demo/playground_scheduler.jsonnet)" &

wait
