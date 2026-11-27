#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; SCRIPT="${DIR}/quickstart.sh"
TMPD="$(mktemp -d)"; export TMPD; trap 'rm -rf "$TMPD"' EXIT
mkdir -p "$TMPD/bin"; export PATH="$TMPD/bin:/usr/bin:/bin"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; method="GET"; wf=""; d=""
for ((i=1; i<=$#; i++)); do
  a="${!i}"; [[ "$a" =~ ^https?:// ]] && url="$a"
  [ "$a" = "-X" ] && { j=$((i+1)); method="${!j}"; }
  [ "$a" = "-w" ] && { j=$((i+1)); wf="${!j}"; }
  [ "$a" = "-d" ] && { j=$((i+1)); d="${!j}"; }
done
echo "$method $url" >> "$TMPD/curl_calls"
[[ "$url" =~ /user$ ]] && { echo '{"login":"autouser"}'; exit 0; }
[[ "$url" =~ /generate$ ]] && { echo "$d" > "$TMPD/last_gen"; touch "$TMPD/generated"; exit 0; }
[[ "$url" =~ /commits/main$ ]] && { [ -f "$TMPD/fail_commits" ] && { [ "$wf" = "%{http_code}" ] && printf "404"; exit 0; }; [ "$wf" = "%{http_code}" ] && printf "200"; exit 0; }
[[ "$url" =~ /repos/ ]] && { [ -f "$TMPD/repo_exists" ] && code="200" || code="404"; [ "$wf" = "%{http_code}" ] && printf "%s" "$code"; exit 0; }
if [[ "$url" =~ fake-k3s ]]; then
  cat << K3S_EOF
#!/bin/sh
touch "$TMPD/k3s_done"
printf '#!/bin/sh\nexit 0\n' > "$TMPD/bin/k3s"
chmod +x "$TMPD/bin/k3s"
K3S_EOF
  exit 0
fi
if [[ "$url" =~ fake-flux ]]; then
  cat << FLUX_EOF
#!/bin/sh
touch "$TMPD/flux_done"
printf '#!/usr/bin/env bash\necho "flux \$*" >> "$TMPD/flux_calls"\nexit 0\n' > "$TMPD/bin/flux"
chmod +x "$TMPD/bin/flux"
FLUX_EOF
  exit 0
fi
exit 1
CURL_EOF
cat << 'KUBECTL_EOF' > "$TMPD/bin/kubectl"
#!/usr/bin/env bash
echo "kubectl $*" >> "$TMPD/kubectl_calls"
[ "${1:-}" = "get" ] && [ "${2:-}" = "namespace" ] && exit 0
[ "${1:-}" = "create" ] && [ "${2:-}" = "secret" ] && { printf "kind: Secret\n"; exit 0; }
[ "${1:-}" = "apply" ] && exit 0
if [ "${1:-}" = "get" ] && [ "${2:-}" = "pod" ]; then
  [ -f "$TMPD/delay" ] && { c=0; [ -f "$TMPD/c" ] && c=$(cat "$TMPD/c"); c=$((c+1)); echo "$c" > "$TMPD/c"; [ "$c" -lt 2 ] && exit 0; }
  printf "eri-restate-7890-test"; exit 0
fi
[ "${1:-}" = "wait" ] && exit 0
[ "${1:-}" = "exec" ] && { echo "$*" > "$TMPD/agy_done"; exit 0; }
exit 0
KUBECTL_EOF
printf '#!/usr/bin/env bash\necho "flux $*" >> "$TMPD/flux_calls"\nexit 0\n' > "$TMPD/bin/flux"
printf '#!/bin/sh\nexit 0\n' > "$TMPD/bin/k3s"; chmod +x "$TMPD/bin/"*
assert() { "$@" || { echo "ASSERT FAILED: $*" >&2; exit 1; }; }
assert_fail() { ! "$@" 2>/dev/null || { echo "EXPECTED FAILURE: $*" >&2; exit 1; }; }
assert "$SCRIPT" --help && assert "$SCRIPT" -h
assert_fail env -u GITHUB_PAT DISCORD_BOT_TOKEN=t "$SCRIPT"
assert_fail env GITHUB_PAT=p -u DISCORD_BOT_TOKEN "$SCRIPT"
assert_fail env GITHUB_PAT=p DISCORD_BOT_TOKEN=t GITHUB_OWNER=bad/owner "$SCRIPT"
assert_fail env GITHUB_PAT=p DISCORD_BOT_TOKEN=t GITHUB_OWNER=o AERIAL_CONFIG_REPO=bad/repo "$SCRIPT"
assert_fail env GITHUB_PAT=p DISCORD_BOT_TOKEN=t GITHUB_OWNER=o AERIAL_TEMPLATE_REPO=no_slash "$SCRIPT"
export GITHUB_PAT="pat123" DISCORD_BOT_TOKEN="bot456" GITHUB_API_URL="http://fake-gh" K3S_INSTALL_URL="http://fake-k3s" FLUX_INSTALL_URL="http://fake-flux" AERIAL_WAIT_TIMEOUT="1s" AERIAL_POLL_SLEEP="0" AERIAL_SKIP_TTY_EXEC="1"
out=$("$SCRIPT")
assert grep -q "Generating from template" <<< "$out" && assert [ -f "$TMPD/generated" ]
assert grep -q '"owner":"autouser"' "$TMPD/last_gen" && assert grep -q "flux bootstrap github --owner=autouser" "$TMPD/flux_calls"
touch "$TMPD/repo_exists"; rm -f "$TMPD/generated"
out2=$("$SCRIPT") && assert grep -q "exists." <<< "$out2" && assert [ ! -f "$TMPD/generated" ]
rm -f "$TMPD/bin/k3s" "$TMPD/bin/flux"; ck="$TMPD/nested/kube/cfg"
out3=$(KUBECONFIG="$ck" "$SCRIPT") && assert [ -d "$(dirname "$ck")" ] && assert [ -f "$TMPD/k3s_done" ] && assert [ -f "$TMPD/flux_done" ]
rm -f "$TMPD/k3s_done" "$TMPD/flux_done"; out4=$("$SCRIPT") && assert [ ! -f "$TMPD/k3s_done" ] && assert [ ! -f "$TMPD/flux_done" ]
touch "$TMPD/delay" && rm -f "$TMPD/c"; "$SCRIPT" >/dev/null && assert [ "$(cat "$TMPD/c")" -ge 2 ]
AERIAL_SKIP_TTY_EXEC=0 AERIAL_FORCE_TTY_EXEC=1 "$SCRIPT" >/dev/null && assert [ -f "$TMPD/agy_done" ]
touch "$TMPD/fail_commits" && rm -f "$TMPD/repo_exists" && assert_fail "$SCRIPT"
echo "ok - quickstart hermetic tests passed"
