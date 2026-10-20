#!/usr/bin/env bash
# Build once, then use separate real CLI processes against a persistent cluster.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
scenario_root=${1:?usage: examples/local-hsm.sh /absolute/dedicated-workdir}
mkdir -p -- "$scenario_root"
scenario_root=$(cd -- "$scenario_root" && pwd)
if test -e "$scenario_root/cluster" || test -e "$scenario_root/restored.bin"; then
    echo "Use a new dedicated workdir; existing data is retained." >&2
    exit 1
fi
cd -- "$repo_dir"
cargo build --locked -p hsm-curvine --features native-integration --bin hsm-curvine -j 6
scenario_target=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
scenario_bin=$scenario_target/debug/hsm-curvine
"$scenario_bin" serve-local "$scenario_root/cluster" > "$scenario_root/servers.log" 2>&1 &
scenario_pid=$!
scenario_stop() {
    kill "$scenario_pid" 2>/dev/null || true
    for ((scenario_stop_i=0; scenario_stop_i<20; scenario_stop_i++)); do
        if ! kill -0 "$scenario_pid" 2>/dev/null; then break; fi
        sleep 0.1
    done
    kill -KILL "$scenario_pid" 2>/dev/null || true
    wait "$scenario_pid" 2>/dev/null || true
}
trap scenario_stop EXIT
for ((scenario_i=0; scenario_i<180; scenario_i++)); do
    if test -f "$scenario_root/cluster/client.json"; then break; fi
    if ! kill -0 "$scenario_pid" 2>/dev/null; then cat "$scenario_root/servers.log" >&2; exit 1; fi
    sleep 1
done
test -f "$scenario_root/cluster/client.json"
scenario_cli=("$scenario_bin" --config "$scenario_root/cluster/client.json" --archive-root "$scenario_root/archive" --backend-id local)
python3 - "$scenario_root/input.bin" <<'PY'
import sys
with open(sys.argv[1], 'wb') as payload:
    payload.write(bytes(range(251)) * 12534 + b'local HSM\n')
PY
"${scenario_cli[@]}" --op-id demo-import import "$scenario_root/input.bin" /.hsm/demo-file
for scenario_action in archive release; do
    "${scenario_cli[@]}" policy-put "$repo_dir/examples/local-policies/$scenario_action.json"
    "${scenario_cli[@]}" worker --once
    "${scenario_cli[@]}" status /.hsm/demo-file
done
if "${scenario_cli[@]}" read /.hsm/demo-file "$scenario_root/offline-read.bin"; then
    echo 'Offline access unexpectedly succeeded' >&2
    exit 1
fi
"${scenario_cli[@]}" gc /.hsm/demo-file
"${scenario_cli[@]}" policy-put "$repo_dir/examples/local-policies/recall.json"
"${scenario_cli[@]}" worker --once
"${scenario_cli[@]}" read /.hsm/demo-file "$scenario_root/restored.bin"
cmp -- "$scenario_root/input.bin" "$scenario_root/restored.bin"
"${scenario_cli[@]}" worker --once
"${scenario_cli[@]}" jobs > "$scenario_root/jobs.jsonl"
echo "Verified policy -> intent -> data-mover -> archive/release/recall; exact bytes restored. Results: $scenario_root"
