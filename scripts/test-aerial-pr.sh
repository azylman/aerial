#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${DIR}/aerial-pr.sh"
[ -f "$SCRIPT" ] || { echo "ERROR: $SCRIPT not found" >&2; exit 1; }
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export TMPD AERIAL_SCRATCH_BASE_DIR="$TMPD/scratch" AERIAL_USE_LOOPBACK_GITHUB=1 PATH="$TMPD/bin:$PATH"
mkdir -p "$TMPD/bin" "$TMPD/repo" "$AERIAL_SCRATCH_BASE_DIR"
git -C "$TMPD/repo" init -b main -q && git -C "$TMPD/repo" config user.email "t@e.com" && git -C "$TMPD/repo" config user.name "T"
touch "$TMPD/repo/f" && git -C "$TMPD/repo" add f && git -C "$TMPD/repo" commit -m "init" -q
git init --bare "$TMPD/origin.git" -q && git -C "$TMPD/repo" remote add origin "$TMPD/origin.git" && git -C "$TMPD/repo" push -u origin main -q
export AERIAL_REPO_URL_OVERRIDE="$TMPD/origin.git"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; for arg in "$@"; do [[ "$arg" =~ http ]] && url="$arg"; done
[[ "$url" =~ /healthz ]] && { printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls\?state=open ]]; then
  if [[ "$url" =~ chunk-422 ]]; then
    [ -f "$TMPD/post_422_seen" ] && echo '[{"number":101,"html_url":"https://github.com/azylman/aerial/pull/101","title":"422 pr"}]' || echo '[]'
  else
    [ -f "$TMPD/has_existing_pr" ] && echo '[{"number":99,"html_url":"https://github.com/azylman/aerial/pull/99","title":"existing"}]' || echo '[]'
  fi
  printf "\n200"; exit 0
fi
[[ "$url" =~ /pulls/98 ]] && { echo '{"state":"closed","html_url":"https://github.com/azylman/aerial/pull/98"}'; printf "\n200"; exit 0; }
[[ "$url" =~ /pulls/[0-9]+ ]] && { echo '{"state":"open","html_url":"https://github.com/azylman/aerial/pull/99","head":{"sha":"1234567890abcdef1234567890abcdef12345678"}}'; printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls ]] && [[ "$*" =~ POST ]]; then
  echo "$*" > "$TMPD/last_post_body"
  if [[ "$*" =~ chunk-422 ]]; then touch "$TMPD/post_422_seen"; echo '{"message":"Validation Failed"}'; printf "\n422"; exit 0; fi
  touch "$TMPD/has_existing_pr"; echo '{"number":99,"html_url":"https://github.com/azylman/aerial/pull/99"}'; printf "\n201"; exit 0
fi
[[ "$url" =~ /comments ]] && { echo '[]'; printf "\n200"; exit 0; }
[[ "$url" =~ /actions/runs/[0-9]+/rerun ]] && { printf "\n201"; exit 0; }
[[ "$url" =~ /actions/runs ]] && { echo '{"workflow_runs":[{"id":888,"status":"completed","name":"RoboRev Code Review","path":".github/workflows/roborev.yml"}]}'; printf "\n200"; exit 0; }
printf "\n200"
CURL_EOF
chmod +x "$TMPD/bin/curl"

res1=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-test)
s_dir=$(echo "$res1" | jq -r .scratch_dir); br=$(echo "$res1" | jq -r .branch)
echo "$res1" | jq -e '.status == "initialized" and (.branch | contains("vanadis-chunk-test"))' >/dev/null

mkdir -p "$s_dir/scripts" && printf '#!/bin/sh\nexit 1\n' > "$s_dir/scripts/verify.sh" && chmod +x "$s_dir/scripts/verify.sh"
echo "hello" > "$s_dir/hello.txt"; echo "Fix description" > "$s_dir/PR_DESCRIPTION.md"
if "$SCRIPT" submit "$s_dir" "feat: fail verify" 2>/dev/null; then echo "Expected verify failure" >&2; exit 1; fi
[ -f "$s_dir/PR_DESCRIPTION.md" ] && [ "$(cat "$s_dir/PR_DESCRIPTION.md")" = "Fix description" ]
rm -rf "$s_dir/scripts"

res2=$("$SCRIPT" submit "$s_dir" "feat: add hello")
echo "$res2" | jq -e '.status == "submitted" and .pr_number == 99' >/dev/null
grep -q "Fix description" "$TMPD/last_post_body"

old_sha=$(git -C "$TMPD/origin.git" rev-parse "refs/heads/$br")
s_behind=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
git -C "$TMPD/repo" fetch origin "$br" -q && git -C "$TMPD/repo" checkout "$br" -q && echo "remote advance" > "$TMPD/repo/adv.txt" && git -C "$TMPD/repo" add adv.txt && git -C "$TMPD/repo" commit -m "remote advance" -q && git -C "$TMPD/repo" push origin "$br" -q
rem_sha=$(git -C "$TMPD/origin.git" rev-parse "refs/heads/$br")
echo "local work" > "$s_behind/local.txt"
res_stacked=$("$SCRIPT" submit "$s_behind" "feat: stacked local commit")
echo "$res_stacked" | jq -e '.status == "submitted"' >/dev/null
git -C "$TMPD/origin.git" merge-base --is-ancestor "$old_sha" "refs/heads/$br"
git -C "$TMPD/origin.git" merge-base --is-ancestor "$rem_sha" "refs/heads/$br"

s_div=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
echo "div local" > "$s_div/div_l.txt" && git -C "$s_div" add div_l.txt && git -C "$s_div" commit -m "div local" -q
git -C "$TMPD/repo" fetch origin "$br" -q && git -C "$TMPD/repo" reset --hard "origin/$br" -q
echo "div rem" > "$TMPD/repo/div_r.txt" && git -C "$TMPD/repo" add div_r.txt && git -C "$TMPD/repo" commit -m "div rem" -q && git -C "$TMPD/repo" push origin "$br" -q
echo "desc" > "$s_div/PR_DESCRIPTION.md"
if "$SCRIPT" submit "$s_div" "feat: submit diverged" 2>/dev/null; then echo "Expected diverged failure" >&2; exit 1; fi
rm -rf "$s_div"

res_422_init=$("$SCRIPT" init --repo aerial --vanadis-chunk chunk-422)
s_dir_422=$(echo "$res_422_init" | jq -r .scratch_dir)
echo "new 422 work" > "$s_dir_422/work.txt"
echo "PR 422 desc" > "$s_dir_422/PR_DESCRIPTION.md"
res_422=$("$SCRIPT" submit "$s_dir_422" "feat: 422 chunk")
echo "$res_422" | jq -e '.status == "submitted" and .pr_number == 101' >/dev/null
[ -f "$TMPD/post_422_seen" ]

s_rf=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
head -c 24000 /dev/zero | tr '\0' 'A' > "$s_rf/GEMINI.md" && echo "desc" > "$s_rf/PR_DESCRIPTION.md"
if "$SCRIPT" submit "$s_rf" "feat: test rule size" 2>/dev/null; then echo "Expected rule size failure" >&2; exit 1; fi
rm -f "$s_rf/GEMINI.md"
if "$SCRIPT" submit "$s_rf" "feat: test missing bfile" --body-file "$TMPD/nonexistent" 2>"$TMPD/bfile.err"; then echo "Expected missing bfile failure" >&2; exit 1; fi
grep -q "body file not found" "$TMPD/bfile.err"
mkdir -p "$s_rf/scripts" && printf '#!/bin/sh\nexit 1\n' > "$s_rf/scripts/verify.sh" && chmod +x "$s_rf/scripts/verify.sh"
git -C "$s_rf" add scripts/verify.sh && git -C "$s_rf" commit -m "with failing verify" -q && pre_head=$(git -C "$s_rf" rev-parse HEAD)
git -C "$TMPD/repo" checkout main -q && echo "main advance" > "$TMPD/repo/m.txt" && git -C "$TMPD/repo" add m.txt && git -C "$TMPD/repo" commit -m "main advance" -q && git -C "$TMPD/repo" push origin main -q
echo "desc" > "$s_rf/PR_DESCRIPTION.md"
if "$SCRIPT" submit "$s_rf" "feat: fail verify after merge" 2>/dev/null; then echo "Expected verify failure after merge" >&2; exit 1; fi
[ "$(git -C "$s_rf" rev-parse HEAD)" = "$pre_head" ]
rm -rf "$s_rf"

cd "$TMPD/repo"
res3=$("$SCRIPT" override-roborev 99 "valid rebuttal")
echo "$res3" | jq -e '.status == "overridden" and .run_id == 888' >/dev/null
if "$SCRIPT" override-roborev 98 "rebuttal on closed PR" 2>/dev/null; then echo "Expected failure on closed PR" >&2; exit 1; fi
if "$SCRIPT" override-roborev notanumber "rebuttal" 2>/dev/null; then echo "Expected failure on non-numeric PR" >&2; exit 1; fi
echo "ok - hermetic aerial-pr test suite passed"
