#!/usr/bin/env bash
set -euo pipefail

EXPECTED_BASE_SHA="f7bfe1b18b9fd264a1b064e19defcfcca851e165"
ARCHIVE_SHA256="31aa29f44e9f41216d588edaa0f85030328bf8ab870c7cd206cdeacd7650340b"
TARGET_BRANCH="agent/rolling-only-refactor-production"

cp scripts/publish_rolling_refactor.py /tmp/publish_rolling_refactor.py
cat scripts/.rolling_payload_v3.part{00..06} \
  | tr -d '\r\n' \
  | base64 --decode \
  > /tmp/rolling_refactor_payload.tar.xz

printf '%s  %s\n' "$ARCHIVE_SHA256" /tmp/rolling_refactor_payload.tar.xz \
  | sha256sum --check -

rm -rf /tmp/rolling_refactor_payload
mkdir -p /tmp/rolling_refactor_payload
tar --extract --xz \
  --file /tmp/rolling_refactor_payload.tar.xz \
  --directory /tmp/rolling_refactor_payload

test -f /tmp/rolling_refactor_payload/payload/manifest.json
test -d /tmp/rolling_refactor_payload/payload/overlay

python - <<'PY'
import json
from pathlib import Path
manifest = json.loads(
    Path('/tmp/rolling_refactor_payload/payload/manifest.json')
    .read_text(encoding='utf-8')
)
assert manifest['expected_base_sha'] == 'f7bfe1b18b9fd264a1b064e19defcfcca851e165'
assert manifest['target_branch'] == 'agent/rolling-only-refactor-production'
assert len(manifest['overlay_files']) == 44
assert len(manifest['delete_paths']) == 31
print('payload manifest OK')
PY

git fetch --no-tags origin master
actual_master_sha="$(git rev-parse origin/master)"
if [[ "$actual_master_sha" != "$EXPECTED_BASE_SHA" ]]; then
  echo "master moved: expected=$EXPECTED_BASE_SHA actual=$actual_master_sha" >&2
  exit 1
fi

if git ls-remote --exit-code --heads \
  origin "refs/heads/$TARGET_BRANCH" >/dev/null 2>&1; then
  echo "Target branch already exists: $TARGET_BRANCH" >&2
  exit 1
fi

git switch --force-create "$TARGET_BRANCH" "$EXPECTED_BASE_SHA"
test "$(git rev-parse HEAD)" = "$EXPECTED_BASE_SHA"
test -z "$(git status --porcelain=v1)"

python /tmp/publish_rolling_refactor.py \
  --repo "$GITHUB_WORKSPACE" \
  --payload /tmp/rolling_refactor_payload/payload

test ! -e ib_job_data
test ! -e run_job_data.py
test ! -e ib_execution/slot_loss_extension.py
test ! -e ib_execution/slot_close_recovery.py
test ! -e scripts/.rolling_payload_v3.part00
test ! -e scripts/publish_rolling_refactor.py
test ! -e scripts/run_rolling_refactor_publisher.sh
test ! -e .github/workflows/publish-rolling-refactor.yml
test ! -e .github/workflows/publish-rolling-refactor-pr.yml

changed_count="$(git diff --name-only "$EXPECTED_BASE_SHA" | wc -l | tr -d ' ')"
if [[ "$changed_count" != "75" ]]; then
  echo "Unexpected changed-file count: $changed_count (expected 75)" >&2
  git diff --stat "$EXPECTED_BASE_SHA"
  exit 1
fi

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install ruff

export IB_ACCOUNT_ID=U0000000
export TELEGRAM_BOT_TOKEN=test-token
export TELEGRAM_CHAT_ID=-1000000000000

python scripts/verify_rolling_refactor.py .
python -m unittest \
  tester.rolling_signal_smoke_tester \
  tester.rolling_price_pipeline_tester \
  -v
python -m compileall -q .
git diff --check

mapfile -d '' changed_python < <(
  git diff --name-only --diff-filter=ACMR -z "$EXPECTED_BASE_SHA" -- '*.py'
)
if (( ${#changed_python[@]} > 0 )); then
  ruff check --ignore E402 "${changed_python[@]}"
fi

python - <<'PY'
import importlib
modules = (
    'core.price_source',
    'ib_signal.signal_candidates',
    'ib_signal.signal_runner',
    'ib_trader.trade_decision_service',
    'ib_execution.emergency_close',
    'ib_execution.execution_loop',
    'wt_run.launcher',
)
for module in modules:
    importlib.import_module(module)
    print('import OK:', module)
PY

git config user.name "OpenAI Agent"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add -A
git diff --cached --check

staged_count="$(git diff --cached --name-only | wc -l | tr -d ' ')"
if [[ "$staged_count" != "75" ]]; then
  echo "Unexpected staged-file count: $staged_count (expected 75)" >&2
  git diff --cached --stat
  exit 1
fi

git commit -m "refactor: simplify robot to rolling-only price pipeline"
commit_sha="$(git rev-parse HEAD)"
git push --set-upstream origin "HEAD:refs/heads/$TARGET_BRANCH"

{
  echo "## Rolling-only production branch published"
  echo
  echo "- Base: \`$EXPECTED_BASE_SHA\`"
  echo "- Commit: \`$commit_sha\`"
  echo "- Branch: \`$TARGET_BRANCH\`"
  echo "- Files changed: $staged_count"
  echo "- Static verifier: passed"
  echo "- Smoke/integration tests: passed"
  echo "- compileall: passed"
  echo "- ruff: passed"
} >> "$GITHUB_STEP_SUMMARY"
