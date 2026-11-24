#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${DIR}/aerial-pr.sh"
[ -f "$SCRIPT" ] || SCRIPT="/tmp/test-compact7-aerial-pr.sh"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export AERIAL_SCRATCH_BASE_DIR="$TMPD/scratch" AERIAL_USE_LOOPBACK_GITHUB=1 PATH="$TMPD/bin:$PATH"
mkdir -p "$TMPD/bin" "$TMPD/repo" "$AERIAL_SCRATCH_BASE_DIR"
git -C "$TMPD/repo" init -b main -q && git -C "$TMPD/repo" config user.email "t@e.com" && git -C "$TMPD/repo" config user.name "T"
touch "$TMPD/repo/f" && git -C "$TMPD/repo" add f && git -C "$TMPD/repo" commit -m "init" -q
git init --bare "$TMPD/origin.git" -q && git -C "$TMPD/repo" remote add origin "$TMPD/origin.git" && git -C "$TMPD/repo" push -u origin main -q
export AERIAL_REPO_URL_OVERRIDE="$TMPD/origin.git"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; for arg in "$@"; do [[ "$arg" =~ http ]] && url="$arg"; done
[[ "$url" =~ /healthz ]] && { printf "\n200"; exit 0; }
[[ "$url" =~ /pulls\?state=open ]] && { echo '[{"number":99,"html_url":"https://github.com/azylman/aerial/pull/99","title":"existing"}]'; printf "\n200"; exit 0; }
[[ "$url" =~ /pulls/[0-9]+ ]] && { echo '{"state":"open","html_url":"https://github.com/azylman/aerial/pull/99","head":{"sha":"1234567890abcdef1234567890abcdef12345678"}}'; printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls ]] && [[ "$*" =~ POST ]]; then
  if [ -f "$TMPD/fail_post_pr" ]; then echo '{"message":"Validation Failed"}'; printf "\n422"; exit 0; fi
  echo '{"number":99,"html_url":"https://github.com/azylman/aerial/pull/99"}'; printf "\n201"; exit 0
fi
[[ "$url" =~ /comments ]] && { echo '[]'; printf "\n200"; exit 0; }
[[ "$url" =~ /actions/runs/[0-9]+/rerun ]] && { printf "\n201"; exit 0; }
[[ "$url" =~ /actions/runs ]] && { echo '{"workflow_runs":[{"id":888,"status":"completed","name":"RoboRev Code Review"}]}'; printf "\n200"; exit 0; }
printf "\n200"
CURL_EOF
chmod +x "$TMPD/bin/curl"

res1=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-test)
echo "$res1" | jq -e '.status == "initialized" and (.branch | contains("vanadis-chunk-test"))' >/dev/null
s_dir=$(echo "$res1" | jq -r .scratch_dir)

echo "hello" > "$s_dir/hello.txt"
echo "Fix description" > "$s_dir/PR_DESCRIPTION.md"
res2=$("$SCRIPT" submit "$s_dir" "feat: add hello")
echo "$res2" | jq -e '.status == "submitted" and .pr_number == 99' >/dev/null

touch "$TMPD/fail_post_pr"
res_reuse=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-test)
s_dir2=$(echo "$res_reuse" | jq -r .scratch_dir)
echo "more changes" >> "$s_dir2/hello.txt"
res2_reuse=$("$SCRIPT" submit "$s_dir2" "feat: update hello")
echo "$res2_reuse" | jq -e '.status == "submitted" and .pr_number == 99' >/dev/null

cd "$TMPD/repo"
res3=$("$SCRIPT" override-roborev 99 "valid rebuttal")
echo "$res3" | jq -e '.status == "overridden" and .run_id == 888' >/dev/null

if "$SCRIPT" override-roborev notanumber "rebuttal" 2>/dev/null; then echo "Expected failure on non-numeric PR" >&2; exit 1; fi
echo "ok - hermetic aerial-pr test suite passed"
