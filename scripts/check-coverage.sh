#!/bin/sh
set -eu

# scripts/check-coverage.sh - Native Monorepo Test Coverage & Threshold Gating Engine
# Zero external SaaS dependencies. Statement-weighted Go coverage + Node.js frontend coverage.

CHECK_MODE=0
SUMMARY_MODE=0
TARGET_SERVICE=""
CUSTOM_PROFILE_DIR=""

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --check               Exit with status 1 if any coverage threshold is violated
  --summary             Output Markdown summary table (also written to \$GITHUB_STEP_SUMMARY if set)
  --service <name>      Target a specific microservice (brain, scheduler-mcp, discord-mcp, dashboard, sidecars/gitsync)
  --profile-dir <dir>   Store intermediate coverage profiles in the specified directory
  -h, --help            Show this help message
EOF
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --check)
            CHECK_MODE=1
            shift
            ;;
        --summary)
            SUMMARY_MODE=1
            shift
            ;;
        --service)
            TARGET_SERVICE="$2"
            shift 2
            ;;
        --profile-dir)
            CUSTOM_PROFILE_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Determine Profile Directory
TEMP_CREATED=0
if [ -n "$CUSTOM_PROFILE_DIR" ]; then
    PROF_DIR="$CUSTOM_PROFILE_DIR"
    mkdir -p "$PROF_DIR"
else
    PROF_DIR=$(mktemp -d /tmp/aerial-coverage.XXXXXX)
    TEMP_CREATED=1
fi

cleanup() {
    if [ "$TEMP_CREATED" -eq 1 ] && [ -d "$PROF_DIR" ]; then
        rm -rf "$PROF_DIR"
    fi
}
trap cleanup EXIT INT TERM

# ANSI Colors
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

echo "${CYAN}⚡ [Aerial Coverage] Starting monorepo coverage measurement...${NC}"

# Define Services to Check
if [ -n "$TARGET_SERVICE" ]; then
    ALL_GO_SERVICES="$TARGET_SERVICE"
else
    ALL_GO_SERVICES="brain scheduler-mcp discord-mcp dashboard sidecars/gitsync"
fi

# Target Minimum Thresholds
GLOBAL_GO_THRESHOLD=70.0
FRONTEND_LINE_THRESHOLD=90.0

# Critical backend package thresholds (Key -> Floor)
# brain/pkg/classifier: 90%
# brain/pkg/config:     80%
# brain/pkg/db:         70%
# brain/pkg/memory:     70%
# brain/pkg/queue:      75%
# brain/pkg/sanitizer:  90%
# brain/pkg/watcher:    80%

get_pkg_threshold() {
    case "$1" in
        *brain/pkg/classifier*) echo "90.0" ;;
        *brain/pkg/config*)     echo "80.0" ;;
        *brain/pkg/db*)         echo "70.0" ;;
        *brain/pkg/memory*)     echo "70.0" ;;
        *brain/pkg/queue*)      echo "75.0" ;;
        *brain/pkg/sanitizer*)  echo "90.0" ;;
        *brain/pkg/watcher*)    echo "80.0" ;;
        *)                      echo "0.0" ;;
    esac
}

VIOLATIONS=""
record_violation() {
    msg="$1"
    if [ -z "$VIOLATIONS" ]; then
        VIOLATIONS="$msg"
    else
        VIOLATIONS="${VIOLATIONS}
$msg"
    fi
}

# Collect Go Coverage per Package
PKG_DATA_FILE="${PROF_DIR}/pkg_summary.txt"
: > "$PKG_DATA_FILE"

TOTAL_GO_STMTS=0
COVERED_GO_STMTS=0

for svc in $ALL_GO_SERVICES; do
    if [ ! -d "$svc" ]; then
        continue
    fi

    # Discover all packages within service
    pkgs=$( (cd "$svc" && go list ./... 2>/dev/null) || true )
    if [ -z "$pkgs" ]; then
        continue
    fi

    for pkg in $pkgs; do
        safe_pkg=$(echo "$pkg" | tr '/.' '__')
        prof_file="${PROF_DIR}/${safe_pkg}.out"

        # Execute go test with isolated coverprofile
        (cd "$svc" && env -u DATABASE_URL go test -coverprofile="$prof_file" "$pkg" >/dev/null 2>&1) || true

        pkg_tot=0
        pkg_cov=0
        if [ -f "$prof_file" ]; then
            # Parse coverage.out with awk
            counts=$(awk '
                BEGIN { tot=0; cov=0 }
                /^mode:/ { next }
                NF >= 3 {
                    stmts = $(NF-1) + 0
                    count = $NF + 0
                    tot += stmts
                    if (count > 0) cov += stmts
                }
                END { printf("%d %d\n", tot, cov) }
            ' "$prof_file")

            pkg_tot=$(echo "$counts" | awk '{print $1}')
            pkg_cov=$(echo "$counts" | awk '{print $2}')
        fi

        thresh=$(get_pkg_threshold "$pkg")
        pct_val="-1"
        status="PASS"

        if [ "$pkg_tot" -gt 0 ]; then
            pct_val=$(awk "BEGIN { printf \"%.1f\", ($pkg_cov / $pkg_tot) * 100.0 }")
            TOTAL_GO_STMTS=$((TOTAL_GO_STMTS + pkg_tot))
            COVERED_GO_STMTS=$((COVERED_GO_STMTS + pkg_cov))

            # Check threshold violation if a floor is defined
            is_violation=$(awk "BEGIN { print ($pct_val < $thresh) ? 1 : 0 }")
            if [ "$is_violation" -eq 1 ]; then
                status="FAIL"
                record_violation "Package $pkg coverage ($pct_val%) is below required floor (${thresh}%)"
            fi
        else
            status="N/A"
        fi

        echo "${svc}|${pkg}|${pkg_tot}|${pkg_cov}|${pct_val}|${thresh}|${status}" >> "$PKG_DATA_FILE"
    done
done

# Calculate Global Go Coverage
GLOBAL_GO_PCT="0.0"
GLOBAL_GO_STATUS="FAIL"
if [ "$TOTAL_GO_STMTS" -gt 0 ]; then
    GLOBAL_GO_PCT=$(awk "BEGIN { printf \"%.2f\", ($COVERED_GO_STMTS / $TOTAL_GO_STMTS) * 100.0 }")
    is_global_violation=$(awk "BEGIN { print ($GLOBAL_GO_PCT < $GLOBAL_GO_THRESHOLD) ? 1 : 0 }")
    if [ "$is_global_violation" -eq 0 ]; then
        GLOBAL_GO_STATUS="PASS"
    else
        record_violation "Global Go Backend statement coverage ($GLOBAL_GO_PCT%) is below threshold (${GLOBAL_GO_THRESHOLD}%)"
    fi
fi

# Measure Frontend Coverage (Permet HUD)
FRONTEND_LINE_PCT="0.0"
FRONTEND_BRANCH_PCT="0.0"
FRONTEND_FUNCS_PCT="0.0"
FRONTEND_STATUS="SKIPPED"

if [ -f "dashboard/app.test.js" ] && { [ -z "$TARGET_SERVICE" ] || [ "$TARGET_SERVICE" = "dashboard" ]; }; then
    if command -v node >/dev/null 2>&1; then
        node_out=$( (cd dashboard && node --test --experimental-test-coverage *.test.js 2>&1) || true )
        
        # Parse coverage table from node output
        cov_line=$(echo "$node_out" | grep -E "ℹ all files\s+\|" | head -n 1)
        if [ -n "$cov_line" ]; then
            FRONTEND_LINE_PCT=$(echo "$cov_line" | awk -F'|' '{gsub(/[ %]/, "", $2); print $2}')
            FRONTEND_BRANCH_PCT=$(echo "$cov_line" | awk -F'|' '{gsub(/[ %]/, "", $3); print $3}')
            FRONTEND_FUNCS_PCT=$(echo "$cov_line" | awk -F'|' '{gsub(/[ %]/, "", $4); print $4}')

            is_fe_violation=$(awk "BEGIN { print ($FRONTEND_LINE_PCT < $FRONTEND_LINE_THRESHOLD) ? 1 : 0 }")
            if [ "$is_fe_violation" -eq 0 ]; then
                FRONTEND_STATUS="PASS"
            else
                FRONTEND_STATUS="FAIL"
                record_violation "Permet HUD Frontend line coverage ($FRONTEND_LINE_PCT%) is below threshold (${FRONTEND_LINE_THRESHOLD}%)"
            fi
        fi
    fi
fi

# Print Console Table
printf "\n%bMonorepo Test Coverage Summary%b\n" "$BOLD" "$NC"
printf "%-18s | %-54s | %-12s | %-9s | %-7s | %-6s\n" "Service" "Package" "Statements" "Coverage" "Floor" "Status"
printf -- "------------------------------------------------------------------------------------------------------------------------------------\n"

while IFS='|' read -r svc pkg tot cov pct thresh status; do
    if [ "$tot" -eq 0 ]; then
        cov_str="N/A"
        floor_str="-"
    else
        cov_str="${pct}%"
        floor_str="${thresh}%"
    fi
    
    if [ "$status" = "PASS" ]; then
        status_colored="${GREEN}PASS${NC}"
    elif [ "$status" = "FAIL" ]; then
        status_colored="${RED}FAIL${NC}"
    else
        status_colored="${YELLOW}N/A${NC}"
    fi

    printf "%-18s | %-54s | %4d / %4d   | %-9s | %-7s | %b\n" "$svc" "$pkg" "$cov" "$tot" "$cov_str" "$floor_str" "$status_colored"
done < "$PKG_DATA_FILE"

printf -- "------------------------------------------------------------------------------------------------------------------------------------\n"
if [ "$GLOBAL_GO_STATUS" = "PASS" ]; then
    printf "%bGlobal Go Statement Coverage: %s%% (%d/%d statements) [Floor: %s%%] - PASS%b\n" "$GREEN" "$GLOBAL_GO_PCT" "$COVERED_GO_STMTS" "$TOTAL_GO_STMTS" "$GLOBAL_GO_THRESHOLD" "$NC"
else
    printf "%bGlobal Go Statement Coverage: %s%% (%d/%d statements) [Floor: %s%%] - FAIL%b\n" "$RED" "$GLOBAL_GO_PCT" "$COVERED_GO_STMTS" "$TOTAL_GO_STMTS" "$GLOBAL_GO_THRESHOLD" "$NC"
fi

if [ "$FRONTEND_STATUS" != "SKIPPED" ]; then
    if [ "$FRONTEND_STATUS" = "PASS" ]; then
        printf "%bPermet HUD Frontend Coverage: %s%% lines, %s%% branches, %s%% funcs [Floor: %s%%] - PASS%b\n" "$GREEN" "$FRONTEND_LINE_PCT" "$FRONTEND_BRANCH_PCT" "$FRONTEND_FUNCS_PCT" "$FRONTEND_LINE_THRESHOLD" "$NC"
    else
        printf "%bPermet HUD Frontend Coverage: %s%% lines, %s%% branches, %s%% funcs [Floor: %s%%] - FAIL%b\n" "$RED" "$FRONTEND_LINE_PCT" "$FRONTEND_BRANCH_PCT" "$FRONTEND_FUNCS_PCT" "$FRONTEND_LINE_THRESHOLD" "$NC"
    fi
fi
printf "\n"

# Markdown Summary Generation (for GitHub Step Summary or --summary)
generate_markdown_summary() {
    cat <<EOF
## ⚡ Aerial Monorepo Test Coverage & Gating Summary

### 📊 Global Target Metrics

| Metric | Measured | Target Floor | Status |
| :--- | :---: | :---: | :---: |
| **Global Go Backend Statement Coverage** | **${GLOBAL_GO_PCT}%** (${COVERED_GO_STMTS}/${TOTAL_GO_STMTS} stmts) | \`>= ${GLOBAL_GO_THRESHOLD}%\` | $([ "$GLOBAL_GO_STATUS" = "PASS" ] && echo "✅ PASS" || echo "❌ FAIL") |
| **Permet HUD Frontend Logic Coverage** | **${FRONTEND_LINE_PCT}%** lines (${FRONTEND_BRANCH_PCT}% branch, ${FRONTEND_FUNCS_PCT}% func) | \`>= ${FRONTEND_LINE_THRESHOLD}%\` | $([ "$FRONTEND_STATUS" = "PASS" ] && echo "✅ PASS" || echo "❌ FAIL") |

### 📦 Package-Level Statement Coverage Breakdown

| Service | Package | Covered / Total Stmts | Coverage | Min Floor | Status |
| :--- | :--- | :---: | :---: | :---: | :---: |
EOF

    while IFS='|' read -r svc pkg tot cov pct thresh status; do
        if [ "$tot" -eq 0 ]; then
            cov_display="N/A"
            floor_display="-"
            status_icon="⚪ N/A"
        else
            cov_display="\`${pct}%\`"
            if [ "$thresh" = "0.0" ]; then
                floor_display="-"
            else
                floor_display="\`>= ${thresh}%\`"
            fi
            if [ "$status" = "PASS" ]; then
                status_icon="✅ PASS"
            else
                status_icon="❌ FAIL"
            fi
        fi
        echo "| \`$svc\` | \`$pkg\` | $cov / $tot | $cov_display | $floor_display | $status_icon |"
    done < "$PKG_DATA_FILE"
}

if [ "$SUMMARY_MODE" -eq 1 ] || [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    MD_CONTENT=$(generate_markdown_summary)
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
        echo "$MD_CONTENT" >> "$GITHUB_STEP_SUMMARY"
    fi
    if [ "$SUMMARY_MODE" -eq 1 ] && [ -z "${GITHUB_STEP_SUMMARY:-}" ]; then
        echo "$MD_CONTENT"
    fi
fi

# Exit status handling
if [ "$CHECK_MODE" -eq 1 ]; then
    if [ -n "$VIOLATIONS" ]; then
        echo "${RED}🚨 [Aerial Coverage] Coverage threshold check failed with the following violations:${NC}" >&2
        echo "$VIOLATIONS" | while read -r line; do
            echo "   • $line" >&2
        done
        exit 1
    fi
    echo "${GREEN}✅ [Aerial Coverage] All monorepo coverage thresholds satisfied successfully!${NC}"
fi

exit 0
