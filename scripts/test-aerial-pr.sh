#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; SCRIPT="${DIR}/aerial-pr.sh"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export TMPD AERIAL_SCRATCH_BASE_DIR="$TMPD/scratch" AERIAL_USE_LOOPBACK_GITHUB=1 PATH="$TMPD/bin:$PATH"
mkdir -p "$TMPD/bin" "$TMPD/repo" "$AERIAL_SCRATCH_BASE_DIR"
git -C "$TMPD/repo" init -b main -q && git -C "$TMPD/repo" config user.email "t@e.com" && git -C "$TMPD/repo" config user.name "T"
touch "$TMPD/repo/f" && git -C "$TMPD/repo" add f && git -C "$TMPD/repo" commit -m "init" -q
git init --bare "$TMPD/origin.git" -q && git -C "$TMPD/repo" remote add origin "$TMPD/origin.git" && git -C "$TMPD/repo" push -u origin main -q
export AERIAL_REPO_URL_OVERRIDE="file://$TMPD/origin.git"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; for a in "$@"; do [[ "$a" =~ http ]] && url="$a"; [[ "$a" =~ ^Authorization:\ (.*) ]] && echo "$a" > "$TMPD/last_auth_hdr"; done
[[ "$url" =~ /healthz ]] && { printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls\?state=open ]]; then
  [[ "$url" =~ test%2Bbranch ]] && touch "$TMPD/seen_encoded_branch"
  [[ "$url" =~ chunk-emptybody ]] && { echo '[{"number":102,"html_url":"https://github.com/a/b/pull/102","title":"t","body":""}]'; printf "\n200"; exit 0; }
  [[ "$url" =~ chunk-422 ]] && [ -f "$TMPD/p422" ] && echo '[{"number":101,"html_url":"https://github.com/a/b/pull/101","title":"t"}]' || { [[ "$url" =~ chunk-test ]] && [ -f "$TMPD/has_pr" ] && echo '[{"number":99,"html_url":"https://github.com/a/b/pull/99","title":"t","body":"existing"}]' || echo '[]'; }
  printf "\n200"; exit 0
fi
[[ "$url" =~ /pulls/98 ]] && { echo '{"state":"closed","html_url":"https://github.com/a/b/pull/98"}'; printf "\n200"; exit 0; }
[[ "$url" =~ /pulls/[0-9]+ ]] && { echo '{"state":"open","html_url":"https://github.com/a/b/pull/99","head":{"sha":"1234567890abcdef1234567890abcdef12345678"}}'; printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls ]] && [[ "$*" =~ POST ]]; then
  echo "$*" > "$TMPD/last_post_body"; [[ "$*" =~ chunk-500 ]] && { echo '{"message":"err"}'; printf "\n500"; exit 0; }
  [[ "$*" =~ chunk-422 ]] && { touch "$TMPD/p422"; echo '{"message":"fail"}'; printf "\n422"; exit 0; }
  touch "$TMPD/has_pr"; echo '{"number":99,"html_url":"https://github.com/a/b/pull/99"}'; printf "\n201"; exit 0
fi
[[ "$url" =~ /git/refs/heads/ ]] && [[ "$*" =~ DELETE ]] && { [[ "$url" =~ %2F ]] || touch "$TMPD/del_ref"; printf "\n204"; exit 0; }
[[ "$url" =~ /actions/runs/[0-9]+/rerun ]] && { touch "$TMPD/rerun"; printf "\n201"; exit 0; }
[[ "$url" =~ /actions/runs ]] && { [ -f "$TMPD/run_prog" ] && s="in_progress" || s="completed"; echo "{\"workflow_runs\":[{\"id\":888,\"status\":\"$s\",\"name\":\"RoboRev Code Review\",\"path\":\".github/workflows/roborev.yml\"}]}"; printf "\n200"; exit 0; }
printf "[]\n200"
CURL_EOF
chmod +x "$TMPD/bin/curl"
assert() { "$@" || { echo "ASSERT FAILED: $*" >&2; exit 1; }; }
assert_fail() { ! "$@" 2>/dev/null || { echo "EXPECTED FAILURE: $*" >&2; exit 1; }; }

source "$SCRIPT" --source-only
assert [ "$(norm_repo "")" = "aerial" ] && assert [ "$(norm_repo config)" = "aerial-config" ]
assert_fail "$SCRIPT" init --unknown-flag && assert_fail "$SCRIPT" --branch b submit "$TMPD/repo" "msg"
assert [ "$("$SCRIPT" init --repo=config | jq -r .repo)" = "aerial-config" ]
AERIAL_USE_LOOPBACK_GITHUB=0 GITHUB_PAT="pat99" "$SCRIPT" override-roborev 99 "rebuttal" >/dev/null
assert [ "$(cat "$TMPD/last_auth_hdr")" = "Authorization: token pat99" ]

res1=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-test); s1=$(echo "$res1" | jq -r .scratch_dir); br=$(echo "$res1" | jq -r .branch)
assert echo "$res1" | jq -e '.status == "initialized" and (.branch | contains("vanadis-chunk-test"))'
mkdir -p "$s1/scripts"; printf '#!/bin/sh\nexit 1\n' > "$s1/scripts/verify.sh"; chmod +x "$s1/scripts/verify.sh"
echo "hello" > "$s1/hello.txt"; echo "PR desc" > "$s1/PR_DESCRIPTION.md"
assert_fail "$SCRIPT" submit "$s1" "feat: fail verify"
assert [ -f "$s1/PR_DESCRIPTION.md" ] && rm -rf "$s1/scripts"
res2=$("$SCRIPT" submit "$s1" "feat: add hello" -b "desc from b")
assert echo "$res2" | jq -e '.status == "submitted" and .pr_number == 99' && grep -q "desc from b" "$TMPD/last_post_body"
snc=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
assert echo "$("$SCRIPT" submit "$snc" "feat: nc" -b "nc")" | jq -e '.status == "no_changes"'
senc=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
git -C "$senc" checkout -b "feat/test+branch" -q; echo "enc" > "$senc/enc.txt"
assert echo "$("$SCRIPT" submit "$senc" "feat: enc" -b "enc")" | jq -e '.status == "submitted"' && [ -f "$TMPD/seen_encoded_branch" ]
s_beh=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
git -C "$TMPD/repo" fetch origin "$br" -q && git -C "$TMPD/repo" checkout -b "$br" FETCH_HEAD -q && echo "rem" > "$TMPD/repo/r.txt" && git -C "$TMPD/repo" add r.txt && git -C "$TMPD/repo" commit -m "rem" -q && git -C "$TMPD/repo" push origin "$br" -q && rem_sha=$(git -C "$TMPD/repo" rev-parse HEAD)
echo "loc" > "$s_beh/l.txt" && assert echo "$("$SCRIPT" submit "$s_beh" "feat: stacked")" | jq -e '.status == "submitted"' && assert git -C "$TMPD/origin.git" merge-base --is-ancestor "$rem_sha" "$br"

s422=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-422 | jq -r .scratch_dir)
echo "w" > "$s422/w.txt"; echo "d" > "$s422/PR_DESCRIPTION.md"
assert echo "$("$SCRIPT" submit "$s422" "feat: 422")" | jq -e '.status == "submitted" and .pr_number == 101'
seb=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-emptybody | jq -r .scratch_dir)
echo "w" > "$seb/w.txt" && assert echo "$("$SCRIPT" submit "$seb" "feat: eb")" | jq -e '.status == "submitted" and .pr_number == 102'
s500=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-500 | jq -r .scratch_dir)
echo "w" > "$s500/w.txt"; echo "d" > "$s500/PR_DESCRIPTION.md"; assert_fail "$SCRIPT" submit "$s500" "feat: 500"
assert [ -f "$TMPD/del_ref" ]
cd "$TMPD/repo"; assert echo "$("$SCRIPT" override-roborev 99 "rebuttal")" | jq -e '.status == "overridden" and .run_id == 888'
assert [ -f "$TMPD/rerun" ]
touch "$TMPD/run_prog"; rm -f "$TMPD/rerun"
assert echo "$("$SCRIPT" override-roborev 99 "rebuttal")" | jq -e '.status == "overridden"' && [ ! -f "$TMPD/rerun" ]
assert_fail "$SCRIPT" override-roborev 98 "closed" && assert_fail "$SCRIPT" override-roborev badid "rebuttal"
bash "$DIR/test-aerial-pr-edge.sh"
echo "ok - hermetic aerial-pr test suite passed"
