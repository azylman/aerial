#!/usr/bin/env bash
set -euo pipefail

# Hermetic unit test verifying rebase_staged_changes preserves intermediate remote commits
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AERIAL_PR="${DIR}/../aerial-pr.sh"

REPO_TEST_MODE=1
# shellcheck source=/dev/null
source "${AERIAL_PR}" --source-only 2>/dev/null || true

REMOTE_DIR=$(mktemp -d /tmp/aerial-pr-adv-test-remote.XXXXXX)
WORK_DIR=$(mktemp -d /tmp/aerial-pr-adv-test-work.XXXXXX)
UPSTREAM_DIR=$(mktemp -d /tmp/aerial-pr-adv-test-up.XXXXXX)
trap 'rm -rf "${REMOTE_DIR}" "${WORK_DIR}" "${UPSTREAM_DIR}"' EXIT

git init -q --bare "${REMOTE_DIR}"
git clone -q "${REMOTE_DIR}" "${UPSTREAM_DIR}"
git -C "${UPSTREAM_DIR}" config user.name 'Tester'
git -C "${UPSTREAM_DIR}" config user.email 'tester@example.com'
git -C "${UPSTREAM_DIR}" checkout -q -b main
echo 'base' > "${UPSTREAM_DIR}/base.txt"
git -C "${UPSTREAM_DIR}" add . && git -C "${UPSTREAM_DIR}" commit -q -m 'base'
git -C "${UPSTREAM_DIR}" push -q -u origin main

# Create feature branch on origin with initial commit
git -C "${UPSTREAM_DIR}" checkout -q -b feat/test
echo 'remote-c1' > "${UPSTREAM_DIR}/file1.txt"
git -C "${UPSTREAM_DIR}" add . && git -C "${UPSTREAM_DIR}" commit -q -m 'c1'
git -C "${UPSTREAM_DIR}" push -q -u origin feat/test

# Clone into WORK_DIR at feat/test (at commit 1)
git clone -q -b feat/test "${REMOTE_DIR}" "${WORK_DIR}"
git -C "${WORK_DIR}" config user.name 'Tester'
git -C "${WORK_DIR}" config user.email 'tester@example.com'

# Remote advances with commit 2 while scratch checkout is open
echo 'remote-c2' > "${UPSTREAM_DIR}/file2.txt"
git -C "${UPSTREAM_DIR}" add . && git -C "${UPSTREAM_DIR}" commit -q -m 'c2 remote'
git -C "${UPSTREAM_DIR}" push -q origin feat/test
C2_SHA=$(git -C "${UPSTREAM_DIR}" rev-parse HEAD)

# In WORK_DIR, user stages a local modification
cd "${WORK_DIR}"
echo 'local-c3' > file3.txt
git add file3.txt

# Run rebase_staged_changes
if ! rebase_staged_changes main 'feat: local'; then
    echo "❌ rebase_staged_changes failed" >&2
    exit 1
fi

# Assert file2.txt was preserved and not reverted
if [ ! -f file2.txt ] || [ "$(cat file2.txt)" != 'remote-c2' ]; then
    echo "❌ remote file2.txt was not preserved" >&2
    exit 1
fi

# Assert file2.txt is not staged as deletion
if git diff --cached --name-status | grep -q '^D.*file2\.txt'; then
    echo "❌ remote file2.txt was staged as deletion" >&2
    exit 1
fi

# Assert file3.txt is staged
if ! git diff --cached --name-only | grep -q '^file3\.txt$'; then
    echo "❌ local file3.txt is not staged" >&2
    exit 1
fi

echo "✅ PASS: rebase_staged_changes preserves intermediate remote commits"
