#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${DIR}/aerial-pr.sh"
[ -f "$SCRIPT" ] || { echo "ERROR: $SCRIPT not found" >&2; exit 1; }

TMPD="$(mktemp -d)"
trap 'rm -rf "$TMPD"' EXIT
export TMPD AERIAL_SCRATCH_BASE_DIR="$TMPD/scratch" AERIAL_USE_LOOPBACK_GITHUB=1 PATH="$TMPD/bin:$PATH"
mkdir -p "$TMPD/bin" "$TMPD/repo" "$AERIAL_SCRATCH_BASE_DIR"

git -C "$TMPD/repo" init -b main -q
git -C "$TMPD/repo" config user.email "t@e.com"
git -C "$TMPD/repo" config user.name "T"
touch "$TMPD/repo/f"
git -C "$TMPD/repo" add f
git -C "$TMPD/repo" commit -m "init" -q
git init --bare "$TMPD/origin.git" -q
git -C "$TMPD/repo" remote add origin "$TMPD/origin.git"
git -C "$TMPD/repo" push -u origin main -q
export AERIAL_REPO_URL_OVERRIDE="file://$TMPD/origin.git"

cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""
for arg in "$@"; do
  [[ "$arg" =~ http ]] && url="$arg"
  [[ "$arg" =~ ^Authorization:\ (.*) ]] && echo "$arg" > "$TMPD/last_auth_hdr"
done
[[ "$url" =~ /healthz ]] && { printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls\?state=open ]]; then
  [[ "$url" =~ test%2Bbranch ]] && touch "$TMPD/seen_encoded_branch"
  [[ "$url" =~ chunk-emptybody ]] && { echo '[{"number":102,"html_url":"https://github.com/azylman/aerial/pull/102","title":"empty body title","body":""}]'; printf "\n200"; exit 0; }
  [[ "$url" =~ chunk-422 ]] && { [ -f "$TMPD/post_422_seen" ] && echo '[{"number":101,"html_url":"https://github.com/azylman/aerial/pull/101","title":"422 pr"}]' || echo '[]'; } || { [[ "$url" =~ chunk-test ]] && [ -f "$TMPD/has_existing_pr" ] && echo '[{"number":99,"html_url":"https://github.com/azylman/aerial/pull/99","title":"existing"}]' || echo '[]'; }
  printf "\n200"; exit 0
fi
[[ "$url" =~ /pulls/98 ]] && { echo '{"state":"closed","html_url":"https://github.com/azylman/aerial/pull/98"}'; printf "\n200"; exit 0; }
[[ "$url" =~ /pulls/[0-9]+ ]] && { echo '{"state":"open","html_url":"https://github.com/azylman/aerial/pull/99","head":{"sha":"1234567890abcdef1234567890abcdef12345678"}}'; printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls ]] && [[ "$*" =~ POST ]]; then
  echo "$*" > "$TMPD/last_post_body"
  [[ "$*" =~ chunk-500 ]] && { echo '{"message":"Internal Error"}'; printf "\n500"; exit 0; }
  [[ "$*" =~ chunk-422 ]] && { touch "$TMPD/post_422_seen"; echo '{"message":"Validation Failed"}'; printf "\n422"; exit 0; }
  touch "$TMPD/has_existing_pr"
  echo '{"number":99,"html_url":"https://github.com/azylman/aerial/pull/99"}'
  printf "\n201"; exit 0
fi
[[ "$url" =~ /git/refs/heads/ ]] && [[ "$*" =~ DELETE ]] && { [[ "$url" =~ %2F ]] || touch "$TMPD/deleted_orphan_ref"; printf "\n204"; exit 0; }
[[ "$url" =~ /comments ]] && { echo '[]'; printf "\n200"; exit 0; }
[[ "$url" =~ /actions/runs/[0-9]+/rerun ]] && { touch "$TMPD/rerun_called"; printf "\n201"; exit 0; }
if [[ "$url" =~ /actions/runs ]]; then
  stat="completed"
  [ -f "$TMPD/run_in_prog" ] && stat="in_progress"
  echo "{\"workflow_runs\":[{\"id\":888,\"status\":\"$stat\",\"name\":\"RoboRev Code Review\",\"path\":\".github/workflows/roborev.yml\"}]}"
  printf "\n200"; exit 0
fi
printf "\n200"
CURL_EOF
chmod +x "$TMPD/bin/curl"

assert() { "$@" || { echo "ASSERT FAILED: $*" >&2; exit 1; }; }
assert_fail() { ! "$@" 2>/dev/null || { echo "EXPECTED FAILURE: $*" >&2; exit 1; }; }

test_source_only_and_unit_helpers() {
  echo "Testing --source-only and unit helpers..."
  source "$SCRIPT" --source-only

  assert [ "$(norm_repo "")" = "aerial" ]
  assert [ "$(norm_repo core)" = "aerial" ]
  assert [ "$(norm_repo config)" = "aerial-config" ]
  assert [ "$(norm_repo sidecars)" = "aerial-sidecars" ]
  assert [ "$(norm_repo azylman/aerial.git)" = "aerial" ]
  assert [ "$(norm_repo azylman/aerial-config.git)" = "aerial-config" ]

  local remote_test_dir="$TMPD/remote_test"
  mkdir -p "$remote_test_dir"
  git -C "$remote_test_dir" init -q
  git -C "$remote_test_dir" remote add origin "https://github.com/customowner/customrepo.git"
  (
    cd "$remote_test_dir"
    REPO_OWNER=""; REPO_NAME=""; REPO_EXP=0
    parse_remote
    assert [ "$REPO_OWNER" = "customowner" ]
    assert [ "$REPO_NAME" = "customrepo" ]
  )
}

test_flag_ordering_and_validation() {
  echo "Testing flag ordering and validation..."
  assert_fail "$SCRIPT" init --unknown-flag
  assert_fail "$SCRIPT" --branch my-branch submit "$TMPD/repo" "feat: test"
  assert_fail "$SCRIPT" --body "body text" init --repo aerial

  assert [ "$("$SCRIPT" init --repo=config | jq -r .repo)" = "aerial-config" ]

  git -C "$TMPD/repo" checkout -b "pre-br" -q
  git -C "$TMPD/repo" push -u origin "pre-br" -q
  git -C "$TMPD/repo" checkout main -q

  local res_pre_br
  res_pre_br=$("$SCRIPT" --branch pre-br init --repo aerial)
  assert [ "$(echo "$res_pre_br" | jq -r .branch)" = "pre-br" ]

  local res_short_b
  res_short_b=$("$SCRIPT" -b pre-br init --repo aerial)
  assert [ "$(echo "$res_short_b" | jq -r .branch)" = "pre-br" ]
}

test_github_pat_auth_path() {
  echo "Testing GITHUB_PAT authentication path..."
  rm -f "$TMPD/last_auth_hdr"
  AERIAL_USE_LOOPBACK_GITHUB=0 GITHUB_PAT="pat-verified-999" "$SCRIPT" override-roborev 99 "rebuttal with pat" >/dev/null
  assert [ -f "$TMPD/last_auth_hdr" ]
  assert [ "$(cat "$TMPD/last_auth_hdr")" = "Authorization: token pat-verified-999" ]
}

test_scratch_init_and_submit() {
  echo "Testing scratch init and submit lifecycle..."
  local nonex_base="$TMPD/nonexistent_scratch_base"
  local res_nonex s_nonex
  res_nonex=$(AERIAL_SCRATCH_BASE_DIR="$nonex_base" "$SCRIPT" init --repo aerial)
  assert [ -d "$nonex_base" ]
  s_nonex=$(echo "$res_nonex" | jq -r .scratch_dir)
  assert [ "${s_nonex#$nonex_base}" != "$s_nonex" ]

  local res_tid
  res_tid=$(AERIAL_TARGET_ID=123456789012345678 "$SCRIPT" init --repo aerial)
  assert echo "$res_tid" | jq -e '.branch | contains("-t123456789012345678-")'

  local res1 s_dir br
  res1=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-test)
  s_dir=$(echo "$res1" | jq -r .scratch_dir)
  br=$(echo "$res1" | jq -r .branch)
  assert echo "$res1" | jq -e '.status == "initialized" and (.branch | contains("vanadis-chunk-test"))'

  mkdir -p "$s_dir/scripts"
  printf '#!/bin/sh\nexit 1\n' > "$s_dir/scripts/verify.sh"
  chmod +x "$s_dir/scripts/verify.sh"
  echo "hello" > "$s_dir/hello.txt"
  echo "Fix description" > "$s_dir/PR_DESCRIPTION.md"
  assert_fail "$SCRIPT" submit "$s_dir" "feat: fail verify"
  assert [ -f "$s_dir/PR_DESCRIPTION.md" ]
  assert [ "$(cat "$s_dir/PR_DESCRIPTION.md")" = "Fix description" ]
  rm -rf "$s_dir/scripts"

  local res2
  res2=$("$SCRIPT" submit "$s_dir" "feat: add hello" -b "desc from b")
  assert echo "$res2" | jq -e '.status == "submitted" and .pr_number == 99'
  assert grep -q "desc from b" "$TMPD/last_post_body"

  local s_nc res_nc
  s_nc=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
  res_nc=$("$SCRIPT" submit "$s_nc" "feat: no modifications" -b "no change body")
  assert echo "$res_nc" | jq -e '.status == "no_changes"'

  local s_enc res_enc
  s_enc=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
  git -C "$s_enc" checkout -b "feat/test+branch" -q
  echo "enc" > "$s_enc/enc.txt"
  res_enc=$("$SCRIPT" submit "$s_enc" "feat: encoded branch" -b "encoded body")
  assert echo "$res_enc" | jq -e '.status == "submitted"'
  assert [ -f "$TMPD/seen_encoded_branch" ]

  local old_sha s_behind rem_sha res_stacked
  old_sha=$(git -C "$TMPD/origin.git" rev-parse "refs/heads/$br")
  s_behind=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
  git -C "$TMPD/repo" fetch origin "$br" -q
  git -C "$TMPD/repo" checkout "$br" -q
  echo "remote advance" > "$TMPD/repo/adv.txt"
  git -C "$TMPD/repo" add adv.txt
  git -C "$TMPD/repo" commit -m "remote advance" -q
  git -C "$TMPD/repo" push origin "$br" -q
  rem_sha=$(git -C "$TMPD/origin.git" rev-parse "refs/heads/$br")
  echo "local work" > "$s_behind/local.txt"
  res_stacked=$("$SCRIPT" submit "$s_behind" "feat: stacked local commit")
  assert echo "$res_stacked" | jq -e '.status == "submitted"'
  assert git -C "$TMPD/origin.git" merge-base --is-ancestor "$old_sha" "refs/heads/$br"
  assert git -C "$TMPD/origin.git" merge-base --is-ancestor "$rem_sha" "refs/heads/$br"
  assert [ "$(git -C "$TMPD/origin.git" log -1 --format=%B "refs/heads/$br")" = "feat: stacked local commit" ]

  local s_div
  s_div=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
  echo "div local" > "$s_div/div_l.txt"
  git -C "$s_div" add div_l.txt
  git -C "$s_div" commit -m "div local" -q
  git -C "$TMPD/repo" fetch origin "$br" -q
  git -C "$TMPD/repo" reset --hard "origin/$br" -q
  echo "div rem" > "$TMPD/repo/div_r.txt"
  git -C "$TMPD/repo" add div_r.txt
  git -C "$TMPD/repo" commit -m "div rem" -q
  git -C "$TMPD/repo" push origin "$br" -q
  echo "desc" > "$s_div/PR_DESCRIPTION.md"
  assert_fail "$SCRIPT" submit "$s_div" "feat: submit diverged"
  rm -rf "$s_div"
}

test_edge_cases_and_overrides() {
  echo "Testing PR edge cases and overrides..."
  local res_422_init s_dir_422 res_422
  res_422_init=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-422)
  s_dir_422=$(echo "$res_422_init" | jq -r .scratch_dir)
  echo "new 422 work" > "$s_dir_422/work.txt"
  echo "PR 422 desc" > "$s_dir_422/PR_DESCRIPTION.md"
  res_422=$("$SCRIPT" submit "$s_dir_422" "feat: 422 chunk")
  assert echo "$res_422" | jq -e '.status == "submitted" and .pr_number == 101'
  assert [ -f "$TMPD/post_422_seen" ]

  local res_eb_init s_eb res_eb
  res_eb_init=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-emptybody)
  s_eb=$(echo "$res_eb_init" | jq -r .scratch_dir)
  echo "eb work" > "$s_eb/work.txt"
  res_eb=$("$SCRIPT" submit "$s_eb" "feat: empty body pr")
  assert echo "$res_eb" | jq -e '.status == "submitted" and .pr_number == 102'

  mkdir -p "$TMPD/scratch/stale-scratch.old"
  touch -d "2 days ago" "$TMPD/scratch/stale-scratch.old"
  "$SCRIPT" init --repo aerial >/dev/null
  assert [ ! -d "$TMPD/scratch/stale-scratch.old" ]

  local s_500
  s_500=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-500 | jq -r .scratch_dir)
  echo "500 work" > "$s_500/w.txt"
  echo "desc" > "$s_500/PR_DESCRIPTION.md"
  assert_fail "$SCRIPT" submit "$s_500" "feat: 500 fail"
  assert [ -f "$TMPD/deleted_orphan_ref" ]
  rm -rf "$s_500"

  local s_rf
  s_rf=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
  head -c 24000 /dev/zero | tr '\0' 'A' > "$s_rf/GEMINI.md"
  echo "desc" > "$s_rf/PR_DESCRIPTION.md"
  assert_fail "$SCRIPT" submit "$s_rf" "feat: test rule size"
  rm -f "$s_rf/GEMINI.md"

  assert_fail "$SCRIPT" submit "$s_rf" "feat: test missing bfile" --body-file "$TMPD/nonexistent"

  python3 -c "print('A'*60005)" > "$TMPD/long_body.txt"
  echo "long work" > "$s_rf/long.txt"
  assert "$SCRIPT" submit "$s_rf" "feat: trunc" --body-file "$TMPD/long_body.txt"
  assert grep -q "Aerial Notice" "$TMPD/last_post_body"

  local s_mrg pre_head
  s_mrg=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
  mkdir -p "$s_mrg/scripts"
  printf '#!/bin/sh\nexit 1\n' > "$s_mrg/scripts/verify.sh"
  chmod +x "$s_mrg/scripts/verify.sh"
  git -C "$s_mrg" add scripts/verify.sh
  git -C "$s_mrg" commit -m "with failing verify" -q
  pre_head=$(git -C "$s_mrg" rev-parse HEAD)
  git -C "$TMPD/repo" checkout main -q
  echo "main advance" > "$TMPD/repo/m.txt"
  git -C "$TMPD/repo" add m.txt
  git -C "$TMPD/repo" commit -m "main advance" -q
  git -C "$TMPD/repo" push origin main -q
  echo "desc" > "$s_mrg/PR_DESCRIPTION.md"
  assert_fail "$SCRIPT" submit "$s_mrg" "feat: fail verify after merge"
  assert [ "$(git -C "$s_mrg" rev-parse HEAD)" = "$pre_head" ]
  rm -rf "$s_mrg"

  cd "$TMPD/repo"
  rm -f "$TMPD/rerun_called"
  local res3
  res3=$("$SCRIPT" override-roborev 99 "valid rebuttal")
  assert echo "$res3" | jq -e '.status == "overridden" and .run_id == 888'
  assert [ -f "$TMPD/rerun_called" ]

  assert_fail "$SCRIPT" override-roborev --reason-file "$TMPD/nonexistent" 99
  touch "$TMPD/run_in_prog"
  rm -f "$TMPD/rerun_called"
  local res_inp
  res_inp=$("$SCRIPT" override-roborev 99 "rebuttal with in progress run")
  assert echo "$res_inp" | jq -e '.status == "overridden"'
  assert [ ! -f "$TMPD/rerun_called" ]
  rm -f "$TMPD/run_in_prog"

  assert_fail "$SCRIPT" override-roborev 98 "rebuttal on closed PR"
  assert_fail "$SCRIPT" override-roborev notanumber "rebuttal"
}

test_source_only_and_unit_helpers
test_flag_ordering_and_validation
test_github_pat_auth_path
test_scratch_init_and_submit
test_edge_cases_and_overrides

echo "ok - hermetic aerial-pr test suite passed"
