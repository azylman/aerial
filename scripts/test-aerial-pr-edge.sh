#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; SCRIPT="${DIR}/aerial-pr.sh"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export TMPD AERIAL_SCRATCH_BASE_DIR="$TMPD/scratch" AERIAL_USE_LOOPBACK_GITHUB=1 PATH="$TMPD/bin:$PATH" AERIAL_NO_VANADIS_CHUNK=1
unset VANADIS_CHUNK_ID AERIAL_VANADIS_CHUNK_ID || true
mkdir -p "$TMPD/bin" "$TMPD/repo" "$AERIAL_SCRATCH_BASE_DIR"
git -C "$TMPD/repo" init -b main -q && git -C "$TMPD/repo" config user.email "t@e.com" && git -C "$TMPD/repo" config user.name "T"
touch "$TMPD/repo/f" && git -C "$TMPD/repo" add f && git -C "$TMPD/repo" commit -m "init" -q
git init --bare "$TMPD/origin.git" -q && git -C "$TMPD/repo" remote add origin "$TMPD/origin.git" && git -C "$TMPD/repo" push -u origin main -q
export AERIAL_REPO_URL_OVERRIDE="file://$TMPD/origin.git"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; for a in "$@"; do [[ "$a" =~ http ]] && url="$a"; done
[[ "$url" =~ /healthz ]] && { printf "\n200"; exit 0; }
[[ "$url" =~ /pulls\?state=open ]] && { echo '[]'; printf "\n200"; exit 0; }
if [[ "$url" =~ /pulls ]] && [[ "$*" =~ POST ]]; then
  echo "$*" > "$TMPD/last_post_body"; echo '{"number":99,"html_url":"https://github.com/a/b/pull/99"}'; printf "\n201"; exit 0
fi
printf "[]\n200"
CURL_EOF
chmod +x "$TMPD/bin/curl"
assert() { "$@" || { echo "ASSERT FAILED: $*" >&2; exit 1; }; }
assert_fail() { ! "$@" 2>/dev/null || { echo "EXPECTED FAILURE: $*" >&2; exit 1; }; }

# 1. Stale scratch cleanup
mkdir -p "$TMPD/scratch/stale-scratch.old"
touch -d "2 days ago" "$TMPD/scratch/stale-scratch.old"
"$SCRIPT" init --repo aerial >/dev/null
assert [ ! -d "$TMPD/scratch/stale-scratch.old" ]

# 2. Rule file size rejection (>23040 bytes)
s_rf=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
head -c 24000 /dev/zero | tr '\0' 'A' > "$s_rf/GEMINI.md"
echo "desc" > "$s_rf/PR_DESCRIPTION.md"
assert_fail "$SCRIPT" submit "$s_rf" "feat: oversized rule file"
rm -f "$s_rf/GEMINI.md"

# 3. Missing --body-file rejection
assert_fail "$SCRIPT" submit "$s_rf" "feat: missing body" --body-file "$TMPD/nonexistent"

# 4. Long body truncation (>60000 bytes)
python3 -c "print('A'*60005)" > "$TMPD/long_body.txt"
echo "long work" > "$s_rf/long.txt"
assert "$SCRIPT" submit "$s_rf" "feat: trunc" --body-file "$TMPD/long_body.txt"
assert grep -q "Aerial Notice" "$TMPD/last_post_body"

# 5. Diverged branch rejection
br="feat/test-diverged"
git -C "$TMPD/repo" checkout -b "$br" -q && git -C "$TMPD/repo" push -u origin "$br" -q
s_div1=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
echo "l1" > "$s_div1/l1.txt"; echo "d" > "$s_div1/PR_DESCRIPTION.md"
"$SCRIPT" submit "$s_div1" "feat: first" >/dev/null
s_div2=$("$SCRIPT" init --repo aerial --branch "$br" | jq -r .scratch_dir)
echo "div local" > "$s_div2/div_l.txt" && git -C "$s_div2" add div_l.txt && git -C "$s_div2" commit -m "div local" -q
git -C "$TMPD/repo" checkout "$br" -q && git -C "$TMPD/repo" fetch origin "$br" -q && git -C "$TMPD/repo" reset --hard "origin/$br" -q
echo "div rem" > "$TMPD/repo/div_r.txt" && git -C "$TMPD/repo" add div_r.txt && git -C "$TMPD/repo" commit -m "div rem" -q && git -C "$TMPD/repo" push origin "$br" -q
echo "desc" > "$s_div2/PR_DESCRIPTION.md"
assert_fail "$SCRIPT" submit "$s_div2" "feat: submit diverged"
rm -rf "$s_div2"

# 6. Verify failure after merge preserves local branch HEAD
s_mrg=$("$SCRIPT" init --repo aerial | jq -r .scratch_dir)
mkdir -p "$s_mrg/scripts"; printf '#!/bin/sh\nexit 1\n' > "$s_mrg/scripts/verify.sh"; chmod +x "$s_mrg/scripts/verify.sh"
git -C "$s_mrg" add scripts/verify.sh && git -C "$s_mrg" commit -m "failing verify" -q
pre_head=$(git -C "$s_mrg" rev-parse HEAD)
git -C "$TMPD/repo" checkout main -q && echo "main adv" > "$TMPD/repo/m.txt" && git -C "$TMPD/repo" add m.txt && git -C "$TMPD/repo" commit -m "main adv" -q && git -C "$TMPD/repo" push origin main -q
echo "desc" > "$s_mrg/PR_DESCRIPTION.md"
assert_fail "$SCRIPT" submit "$s_mrg" "feat: fail verify after merge"
assert [ "$(git -C "$s_mrg" rev-parse HEAD)" = "$pre_head" ]

# 7. Target ID format in standalone branch
br_target=$(AERIAL_TARGET_ID=1574989632892829757 "$SCRIPT" init --repo aerial | jq -r .branch)
[[ "$br_target" =~ ^feat/aerial-pr-[0-9]{14}-t1574989632892829757-[0-9a-f]{6}$ ]] || { echo "ASSERT FAILED: br_target format $br_target" >&2; exit 1; }

echo "ok - aerial-pr edge test suite passed"
