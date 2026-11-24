#!/bin/sh
set -eu
CHECK_MODE=0; SUMMARY_MODE=0; GAPS_MODE=0; TARGET_SERVICE=""; CUSTOM_PROFILE_DIR=""
usage() { echo "Usage: $0 [--check] [--summary] [--gaps] [--service <svc>] [--profile-dir <dir>]" >&2; exit 0; }
while [ $# -gt 0 ]; do case "$1" in
    --check) CHECK_MODE=1; shift ;;
    --summary) SUMMARY_MODE=1; shift ;;
    --gaps) GAPS_MODE=1; shift ;;
    --service) TARGET_SERVICE="$2"; shift 2 ;;
    --profile-dir) CUSTOM_PROFILE_DIR="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1" >&2; usage ;;
esac; done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && cd "$REPO_ROOT"
ensure_bazel_build_files "$REPO_ROOT"

TEMP_CREATED=0; [ -n "$CUSTOM_PROFILE_DIR" ] && { PROF_DIR="$CUSTOM_PROFILE_DIR"; mkdir -p "$PROF_DIR"; } || { PROF_DIR=$(mktemp -d "${TMPDIR:-/tmp}/aerial-coverage.XXXXXX"); TEMP_CREATED=1; }
cleanup() { [ "$TEMP_CREATED" -eq 1 ] && [ -d "$PROF_DIR" ] && rm -rf "$PROF_DIR"; }; trap cleanup EXIT INT TERM

echo "⚡ [Aerial Coverage] Starting monorepo coverage measurement..."
DISCOVERED_GO_SERVICES=$(discover_go_services)
[ "$TARGET_SERVICE" = "." ] && TARGET_SERVICE=""
if [ -n "$TARGET_SERVICE" ]; then
    for s in $DISCOVERED_GO_SERVICES; do [ "$s" = "$TARGET_SERVICE" ] || [ "${s##*/}" = "$TARGET_SERVICE" ] && { TARGET_SERVICE="$s"; break; }; done
    ALL_GO_SERVICES="$TARGET_SERVICE"
else ALL_GO_SERVICES="$DISCOVERED_GO_SERVICES"; fi

COVERAGE_THRESHOLD=95.0
get_pkg_threshold() { echo "$COVERAGE_THRESHOLD"; }
VIOLATIONS=""
record_violation() { [ -z "$VIOLATIONS" ] && VIOLATIONS="$1" || VIOLATIONS="${VIOLATIONS}"$'\n'"$1"; }

PKG_DATA_FILE="${PROF_DIR}/pkg_summary.txt"; SVC_PKG_FILE="${PROF_DIR}/svc_pkgs.txt"; SVC_LIST_FILE="${PROF_DIR}/svcs.txt"
: > "$PKG_DATA_FILE"; : > "$SVC_PKG_FILE"; : > "$SVC_LIST_FILE"
go list -f '{{.Dir}}|{{.ImportPath}}' ./... 2>/dev/null | awk -v r="$REPO_ROOT" -v svcs="$ALL_GO_SERVICES" '
BEGIN{n=split(svcs,sa," ");for(i=1;i<=n;i++)sl[i]=sa[i];p=r"/";pl=length(p)}
{i=index($0,"|");if(i==0)next;d=substr($0,1,i-1);pkg=substr($0,i+1);rd=(d==r?".":(substr(d,1,pl)==p?substr(d,pl+1):d))
for(j=1;j<=n;j++){s=sl[j];if(rd==s||index(rd,s "/")==1){print s "|" pkg;break}}}' >> "$SVC_PKG_FILE"
for s in $ALL_GO_SERVICES; do [ -f "$s/go.mod" ] && [ "$s" != "." ] && (cd "$s" && go list ./... 2>/dev/null) | while IFS= read -r p; do echo "${s}|${p}" >> "$SVC_PKG_FILE"; done || true; grep -q "^${s}|" "$SVC_PKG_FILE" 2>/dev/null && echo "$s" >> "$SVC_LIST_FILE"; done

bazel_targets="//..."
if [ -n "$TARGET_SERVICE" ]; then bazel_targets=""; while IFS= read -r s; do [ -n "$s" ] && bazel_targets="$bazel_targets //${s}/..."; done < "$SVC_LIST_FILE"; fi
cov_log="${PROF_DIR}/bazel_cov.log"; b_stat=0; rm -rf "$REPO_ROOT/bazel-testlogs"
# shellcheck disable=SC2086
bazel coverage --keep_going $bazel_targets >"$cov_log" 2>&1 || b_stat=$?
[ "$b_stat" -ne 0 ] && { cat "$cov_log" >&2; record_violation "Bazel coverage failed (exit $b_stat)"; }

COV_FILES_LIST="${PROF_DIR}/cov_files.txt"; : > "$COV_FILES_LIST"
if [ -d "$REPO_ROOT/bazel-testlogs" ]; then
    sp=""; while IFS= read -r s; do [ -n "$s" ] && [ -d "$REPO_ROOT/bazel-testlogs/$s" ] && sp="$sp $REPO_ROOT/bazel-testlogs/$s"; done < "$SVC_LIST_FILE"
    # shellcheck disable=SC2086
    [ -n "$sp" ] && find -L $sp -maxdepth 8 -name "coverage.dat" 2>/dev/null > "$COV_FILES_LIST" || true
fi
awk -v svc_pkg_file="$SVC_PKG_FILE" -v cov_file_list="$COV_FILES_LIST" -v prof_dir="$PROF_DIR" -v pkg_data_file="$PKG_DATA_FILE" -v gaps_mode="$GAPS_MODE" -v custom_prof_dir="$CUSTOM_PROFILE_DIR" -v default_thresh="$COVERAGE_THRESHOLD" -f "$REPO_ROOT/scripts/lib/check-coverage.awk"

TOTAL_GO_STMTS=0; COVERED_GO_STMTS=0
while IFS='|' read -r s p tot cov pct thresh status; do
    [ -n "$s" ] && [ -n "$p" ] || continue
    [ "$status" = "FAIL" ] && record_violation "Package $p coverage ($pct%) below required floor (${thresh}%)"
    [ "$tot" -gt 0 ] && { TOTAL_GO_STMTS=$((TOTAL_GO_STMTS + tot)); COVERED_GO_STMTS=$((COVERED_GO_STMTS + cov)); }
    if [ "$status" = "FAIL" ] || [ "$GAPS_MODE" -eq 1 ]; then
        nd=$(awk "BEGIN{r=int(($tot*$thresh+99)/100);d=r-$cov;print (d>0)?d:0}")
        [ "$status" = "FAIL" ] || [ "$nd" -gt 0 ] && echo "   🔍 [Coverage Audit] $p: $cov / $tot stmts ($pct%). Deficit: $nd stmts to reach ${thresh}%."
    fi
done < "$PKG_DATA_FILE"

GLOBAL_GO_PCT="0.0"; GLOBAL_GO_STATUS="FAIL"
if [ "$TOTAL_GO_STMTS" -gt 0 ]; then
    GLOBAL_GO_PCT=$(awk "BEGIN{printf \"%.2f\", ($COVERED_GO_STMTS/$TOTAL_GO_STMTS)*100.0}")
    [ "$(awk "BEGIN{print ($COVERED_GO_STMTS*100 < $TOTAL_GO_STMTS*$COVERAGE_THRESHOLD)?1:0}")" -eq 0 ] && GLOBAL_GO_STATUS="PASS" || record_violation "Global Go Backend statement coverage ($GLOBAL_GO_PCT%) below threshold (${COVERAGE_THRESHOLD}%)"
fi

CONSOLE_UI_STATUS="SKIPPED"
if [ -f "services/console/ui/package.json" ] && { [ -z "$TARGET_SERVICE" ] || [ "$TARGET_SERVICE" = "services/console" ] || [ "$TARGET_SERVICE" = "console" ]; }; then
    ui_xml="$REPO_ROOT/bazel-testlogs/services/console/ui_test/test.xml"; ui_log="$REPO_ROOT/bazel-testlogs/services/console/ui_test/test.log"
    if grep -q -E '(//services/console:ui_test.*(FAILED|TIMEOUT|NO STATUS)|FAIL.*//services/console:ui_test)' "$cov_log" 2>/dev/null || { [ -f "$ui_xml" ] && grep -q -E '<(failure|error)' "$ui_xml" 2>/dev/null; }; then
        CONSOLE_UI_STATUS="FAIL"; record_violation "Console React UI tests failed or coverage below threshold (${COVERAGE_THRESHOLD}%) in services/console/ui"
    elif [ -f "$ui_xml" ] || [ -f "$ui_log" ]; then CONSOLE_UI_STATUS="PASS"
    else CONSOLE_UI_STATUS="FAIL"; record_violation "Console React UI tests did not execute in services/console/ui"; fi
fi

generate_markdown_summary() {
    printf "## ⚡ Aerial Monorepo Test Coverage & Gating Summary\n### 📊 Global Target Metrics\n"
    printf "| Metric | Measured | Target Floor | Status |\n| :--- | :---: | :---: | :---: |\n"
    printf "| **Global Go Backend Statement Coverage** | **%s%%** (%s/%s stmts) | \`>= %s%%\` | %s |\n" "$GLOBAL_GO_PCT" "$COVERED_GO_STMTS" "$TOTAL_GO_STMTS" "$COVERAGE_THRESHOLD" "$([ "$GLOBAL_GO_STATUS" = "PASS" ] && echo "✅ PASS" || echo "❌ FAIL")"
    printf "| **Console React UI Coverage (Vitest)** | lines, branches, functions, statements | \`>= %s%%\` | %s |\n" "$COVERAGE_THRESHOLD" "$(case "$CONSOLE_UI_STATUS" in PASS) echo "✅ PASS" ;; FAIL) echo "❌ FAIL" ;; *) echo "⏭️ SKIPPED" ;; esac)"
}

[ "$SUMMARY_MODE" -eq 1 ] && generate_markdown_summary
if [ -z "$TARGET_SERVICE" ]; then
    echo "Global Go Statement Coverage: ${GLOBAL_GO_PCT}% (${COVERED_GO_STMTS}/${TOTAL_GO_STMTS} statements) - ${GLOBAL_GO_STATUS}"
    echo "Console React UI Coverage: Vitest thresholds - ${CONSOLE_UI_STATUS}"
else echo "Coverage (${TARGET_SERVICE}): ${GLOBAL_GO_PCT}% (${COVERED_GO_STMTS}/${TOTAL_GO_STMTS} statements) - ${GLOBAL_GO_STATUS}"; fi
[ "$CHECK_MODE" -eq 1 ] && [ -n "$VIOLATIONS" ] && { echo "🚨 Coverage threshold check failed:" >&2; echo "$VIOLATIONS" >&2; exit 1; }
[ "$CHECK_MODE" -eq 1 ] && echo "✅ [Aerial Coverage] All monorepo coverage thresholds satisfied successfully!"
exit 0
