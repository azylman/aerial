#!/bin/sh
set -eu

# Prevent MSYS2 from mangling Docker volume path colons on Windows
export MSYS_NO_PATHCONV=1
export DOCKER_BUILDKIT=1

# Ensure MinGit / MSYS binaries (/usr/bin, /mingw64/bin, /cmd) are in PATH
for p in /usr/bin /mingw64/bin /cmd; do
    if [ -d "$p" ]; then
        case ":$PATH:" in
            *:"$p":*) ;;
            *) PATH="$p:$PATH" ;;
        esac
    fi
done

MODE="full"
for arg in "$@"; do
    case "$arg" in
        --staged)
            MODE="staged"
            ;;
        --full)
            MODE="full"
            ;;
    esac
done

case "$0" in
    */*) SCRIPT_DIR="${0%/*}" ;;
    *) SCRIPT_DIR="." ;;
esac
SCRIPT_DIR="$(cd "$SCRIPT_DIR" && (pwd -W 2>/dev/null || pwd))"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && (pwd -W 2>/dev/null || pwd))"

if [ "$MODE" != "staged" ]; then
    echo "⚡ [Aerial Verify] Running $MODE verification checks..."
fi

# Helper to check command availability
has_cmd() {
    command -v "$1" >/dev/null 2>&1
}

has_docker() {
    has_cmd docker && docker info >/dev/null 2>&1
}

# Check for UTF-8 BOM (\xef\xbb\xbf)
check_utf8_bom() {
    if has_cmd git; then
        BOM_HEX=$(printf '\357\273\277')
        BOM_FILES=$(git grep -I -l "^$BOM_HEX" 2>/dev/null || true)
        if [ -n "$BOM_FILES" ]; then
            echo "🚨 [Aerial Verify] Error: Invalid UTF-8 BOM detected in tracked files:" >&2
            echo "$BOM_FILES" | sed 's/^/   • /' >&2
            exit 1
        fi
    fi
}

check_gofmt() {
    if has_cmd gofmt; then
        local unformatted=""
        if [ "$MODE" = "staged" ]; then
            local go_staged
            go_staged=$(echo "$STAGED_FILES" | grep -E '\.go$' || true)
            if [ -n "$go_staged" ]; then
                unformatted=$(echo "$go_staged" | while read -r gf; do
                    if [ -f "$gf" ]; then gofmt -l "$gf"; fi
                done)
            fi
        else
            unformatted=$(gofmt -l . 2>/dev/null | grep -v -E '(^|/)vendor/' || true)
        fi
        if [ -n "$unformatted" ]; then
            if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
                echo "🚨 [Aerial Verify] Error: Unformatted Go files detected (run gofmt -w or golangci-lint fmt):" >&2
                echo "$unformatted" | sed 's/^/   • /' >&2
                exit 1
            fi
            echo "$unformatted" | while read -r gf; do
                if [ -n "$gf" ] && [ -f "$gf" ]; then
                    gofmt -w "$gf"
                    if [ "$MODE" = "staged" ] && has_cmd git; then
                        git add "$gf"
                    fi
                fi
            done
        fi
    fi
}

# Check that tests never execute main() directly
check_no_main_in_tests() {
    if has_cmd git; then
        MAIN_CALLS=$(git grep -E '\bmain\(\)' 'services/eri/*_test.go' 2>/dev/null || true)
        if [ -n "$MAIN_CALLS" ]; then
            echo "🚨 [Aerial Verify] Error: Direct execution of main() detected in test files:" >&2
            echo "$MAIN_CALLS" | sed 's/^/   • /' >&2
            echo "Tests must use RunBrainApp(ctx, cfg) with explicit test configs instead." >&2
            exit 1
        fi
    fi
}

# Check rule file sizes (GEMINI.md, AGENTS.md) do not exceed 23,040 bytes (22.5 KB)
# to ensure compiled runtime rules stay strictly beneath Antigravity's 24 KB truncation ceiling.
check_rule_file_sizes() {
    MAX_BYTES=23040 # 22.5 KB (leaves 512B buffer for SyncRules frontmatter wrapper)
    for rule_file in "$REPO_ROOT/rules"/*/*.md "$REPO_ROOT/GEMINI.md" "$REPO_ROOT/AGENTS.md"; do
        if [ -f "$rule_file" ]; then
            size=$(wc -c < "$rule_file" 2>/dev/null | tr -d '[:space:]')
            if [ -n "$size" ] && [ "$size" -gt "$MAX_BYTES" ]; then
                base_name="${rule_file##*/}"
                echo "🚨 [Aerial Verify] Error: Rule file '$base_name' exceeds size limit!" >&2
                echo "   Current size: $size bytes (limit: $MAX_BYTES bytes / 22.5 KB)" >&2
                echo "   Antigravity truncates prompt context when compiled rules exceed 23 KB." >&2
                echo "   Action: Condense guidelines or migrate procedural runbooks to custom skills." >&2
                exit 1
            fi
        fi
    done
}

# Dynamically discover all Go modules in the monorepo
GO_SERVICES=$(cd "$REPO_ROOT" && find . -maxdepth 3 -name "go.mod" ! -path "*/.*" ! -path "*/vendor/*" | sed 's|^\./||; s|/go\.mod$||' | sort | tr '\n' ' ' | sed 's/[[:space:]]*$//')

run_golangci_lint() {
    svc="$1"
    targetPkg="${2:-./...}"
    if [ -d "$svc" ]; then
        if [ "$MODE" != "staged" ]; then
            echo "   [golangci-lint] Linting $svc ($targetPkg)..."
        fi
        if has_cmd golangci-lint; then
            export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.27.1}"
            (cd "$svc" && golangci-lint run --allow-parallel-runners --path-prefix="$svc/" --config "$REPO_ROOT/.golangci.yml" "$targetPkg")
        elif has_docker; then
            docker run --rm \
                -v "$REPO_ROOT:/workspace" -w "/workspace/$svc" \
                golangci/golangci-lint:v2.14.0 \
                golangci-lint run --path-prefix="$svc/" --config /workspace/.golangci.yml "$targetPkg"
        else
            echo "🚨 [Aerial Verify] Error: Neither golangci-lint nor docker found in PATH." >&2
            exit 1
        fi
    fi
}

DEADCODE_BIN=""

ensure_deadcode() {
    if [ -n "$DEADCODE_BIN" ]; then
        return 0
    fi
    if has_cmd deadcode; then
        DEADCODE_BIN="deadcode"
        return 0
    fi
    if has_cmd go; then
        gopath="$(go env GOPATH 2>/dev/null || true)"
        if [ -n "$gopath" ] && [ -x "$gopath/bin/deadcode" ]; then
            DEADCODE_BIN="$gopath/bin/deadcode"
            return 0
        fi
        if [ -x "$HOME/go/bin/deadcode" ]; then
            DEADCODE_BIN="$HOME/go/bin/deadcode"
            return 0
        fi
        echo "   [deadcode] Installing deadcode via go install..."
        go install golang.org/x/tools/cmd/deadcode@v0.51.0
        if [ -n "$gopath" ] && [ -x "$gopath/bin/deadcode" ]; then
            DEADCODE_BIN="$gopath/bin/deadcode"
            return 0
        elif [ -x "$HOME/go/bin/deadcode" ]; then
            DEADCODE_BIN="$HOME/go/bin/deadcode"
            return 0
        elif has_cmd deadcode; then
            DEADCODE_BIN="deadcode"
            return 0
        fi
    fi
    echo "🚨 [Aerial Verify] Error: Neither deadcode nor go found in PATH to perform dead code analysis." >&2
    exit 1
}

run_deadcode() {
    svc="$1"
    if [ "${SKIP_DEADCODE:-0}" = "1" ] || [ "${AERIAL_SKIP_DEADCODE:-0}" = "1" ]; then
        if [ "$MODE" != "staged" ]; then
            echo "   [deadcode] Skipping deadcode check for $svc (SKIP_DEADCODE=1)"
        fi
        return 0
    fi
    if [ -d "$svc" ]; then
        if [ "$MODE" != "staged" ]; then
            echo "   [deadcode] Checking $svc for unreachable code (production graph)..."
        fi
        ensure_deadcode
        main_pkgs=$(cd "$svc" && go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./...) || {
            echo "🚨 [Aerial Verify] Error: failed to list main packages in $svc" >&2
            exit 1
        }
        if [ -n "$main_pkgs" ]; then
            # shellcheck disable=SC2086
            dead_prod=$(cd "$svc" && "$DEADCODE_BIN" $main_pkgs 2>&1) || {
                echo "🚨 [Aerial Verify] Error: deadcode (production) execution failed on $svc:" >&2
                echo "$dead_prod" >&2
                exit 1
            }
            if [ -n "$dead_prod" ]; then
                echo "🚨 [Aerial Verify] Unreachable production code or compiled-in test hooks detected in $svc:" >&2
                echo "$dead_prod" | sed 's/^/   • /' >&2
                exit 1
            fi
        fi
    fi
}

run_go_test() {
    svc="$1"
    if [ -d "$svc" ]; then
        echo "   [go test] Testing $svc (clean-room)..."
        cgo_val="${CGO_ENABLED:-}"
        if [ -z "$cgo_val" ]; then
            if has_cmd gcc || has_cmd clang; then
                cgo_val=1
            else
                cgo_val=0
            fi
        fi
        if has_cmd go; then
            (cd "$svc" && env -i \
                ${PATH:+PATH="$PATH"} \
                ${HOME:+HOME="$HOME"} \
                ${GOROOT:+GOROOT="$GOROOT"} \
                ${GOPATH:+GOPATH="$GOPATH"} \
                ${GOCACHE:+GOCACHE="$GOCACHE"} \
                ${GOMODCACHE:+GOMODCACHE="$GOMODCACHE"} \
                ${GOLANGCI_LINT_CACHE:+GOLANGCI_LINT_CACHE="$GOLANGCI_LINT_CACHE"} \
                ${TMPDIR:+TMPDIR="$TMPDIR"} \
                ${SystemRoot:+SystemRoot="$SystemRoot"} \
                ${SYSTEMROOT:+SYSTEMROOT="$SYSTEMROOT"} \
                ${USERPROFILE:+USERPROFILE="$USERPROFILE"} \
                ${HOMEDRIVE:+HOMEDRIVE="$HOMEDRIVE"} \
                ${HOMEPATH:+HOMEPATH="$HOMEPATH"} \
                ${TMP:+TMP="$TMP"} \
                ${TEMP:+TEMP="$TEMP"} \
                ${LOCALAPPDATA:+LOCALAPPDATA="$LOCALAPPDATA"} \
                ${APPDATA:+APPDATA="$APPDATA"} \
                ${COMSPEC:+COMSPEC="$COMSPEC"} \
                ${PATHEXT:+PATHEXT="$PATHEXT"} \
                MSYS_NO_PATHCONV=1 \
                LANG="${LANG:-en_US.UTF-8}" \
                LC_ALL="${LC_ALL:-en_US.UTF-8}" \
                GIT_TERMINAL_PROMPT=0 \
                CGO_ENABLED="$cgo_val" \
                go test ./...)
        elif has_docker; then
            docker run --rm \
                -v aerial-go-cache:/root/.cache/go-build \
                -v aerial-go-pkg:/go/pkg/mod \
                -v "$(pwd)/$svc:/app" -w /app \
                -e CGO_ENABLED=1 \
                golang:1.27.1 env -i \
                PATH="/go/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
                HOME="/root" \
                GOPATH="/go" \
                GOCACHE="/root/.cache/go-build" \
                CGO_ENABLED=1 \
                LANG="en_US.UTF-8" \
                LC_ALL="en_US.UTF-8" \
                go test ./...
        else
            echo "🚨 [Aerial Verify] Error: Neither go nor docker found in PATH." >&2
            exit 1
        fi
    fi
}

run_go_pkg_coverage() {
    svc="$1"
    targetPkg="${2:-.}"
    if [ -d "$svc" ] && has_cmd go; then
        cgo_val="${CGO_ENABLED:-}"
        if [ -z "$cgo_val" ]; then
            if has_cmd gcc || has_cmd clang; then
                cgo_val=1
            else
                cgo_val=0
            fi
        fi
        prof_file=$(mktemp /tmp/aerial-staged-cov.XXXXXX)
        if ! (cd "$svc" && env -i \
            ${PATH:+PATH="$PATH"} \
            ${HOME:+HOME="$HOME"} \
            ${GOROOT:+GOROOT="$GOROOT"} \
            ${GOPATH:+GOPATH="$GOPATH"} \
            ${GOCACHE:+GOCACHE="$GOCACHE"} \
            ${GOMODCACHE:+GOMODCACHE="$GOMODCACHE"} \
            ${GOLANGCI_LINT_CACHE:+GOLANGCI_LINT_CACHE="$GOLANGCI_LINT_CACHE"} \
            ${TMPDIR:+TMPDIR="$TMPDIR"} \
            ${SystemRoot:+SystemRoot="$SystemRoot"} \
            ${SYSTEMROOT:+SYSTEMROOT="$SYSTEMROOT"} \
            ${USERPROFILE:+USERPROFILE="$USERPROFILE"} \
            ${HOMEDRIVE:+HOMEDRIVE="$HOMEDRIVE"} \
            ${HOMEPATH:+HOMEPATH="$HOMEPATH"} \
            ${TMP:+TMP="$TMP"} \
            ${TEMP:+TEMP="$TEMP"} \
            ${LOCALAPPDATA:+LOCALAPPDATA="$LOCALAPPDATA"} \
            ${APPDATA:+APPDATA="$APPDATA"} \
            ${COMSPEC:+COMSPEC="$COMSPEC"} \
            ${PATHEXT:+PATHEXT="$PATHEXT"} \
            MSYS_NO_PATHCONV=1 \
            LANG="${LANG:-en_US.UTF-8}" \
            LC_ALL="${LC_ALL:-en_US.UTF-8}" \
            GIT_TERMINAL_PROMPT=0 \
            CGO_ENABLED="$cgo_val" \
            go test -timeout "${AERIAL_TEST_TIMEOUT:-180s}" -coverprofile="$prof_file" "$targetPkg"); then
            rm -f "$prof_file"
            exit 1
        fi
        if [ -f "$prof_file" ]; then
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
            if [ "$pkg_tot" -gt 0 ]; then
                pct_val=$(awk "BEGIN { printf \"%.1f\", ($pkg_cov / $pkg_tot) * 100.0 }")
                is_violation=$(awk "BEGIN { print ($pct_val < 95.0) ? 1 : 0 }")
                if [ "$is_violation" -eq 1 ]; then
                    echo "🚨 [Aerial Verify] Package $svc ($targetPkg) coverage ($pct_val% — $pkg_cov/$pkg_tot stmts) is below required 95.0% floor." >&2
                    (cd "$svc" && go tool cover -func="$prof_file" 2>/dev/null | awk '$1 !~ /^total:/ && $NF != "100.0%"' | sort -k3 -n | head -n 5 | sed 's/^/   • /') >&2 || true
                    rm -f "$prof_file"
                    exit 1
                fi
            fi
            rm -f "$prof_file"
        fi
    fi
}

run_node_syntax() {
    file="$1"
    if [ -f "$file" ]; then
        echo "   [node --check] Checking syntax of $file..."
        if has_cmd node; then
            node --check "$file"
        elif has_docker; then
            cat "$file" | docker run --rm -i node:22 node --check
        fi
    fi
}

run_node_test() {
    dir="$1"
    test_pattern="$2"
    if [ -d "$dir" ]; then
        echo "   [node --test] Testing in $dir ($test_pattern)..."
        if has_cmd node; then
            (cd "$dir" && node --test $test_pattern)
        elif has_docker; then
            tar -cf - "$dir" | docker run --rm -i node:22 sh -c "tar -xf - && cd $dir && node --test $test_pattern"
        fi
    fi
}

run_json_syntax() {
    file="$1"
    if [ -f "$file" ]; then
        echo "   [json-check] Validating syntax of $file..."
        if has_cmd node; then
            node -e "JSON.parse(require('fs').readFileSync('$file', 'utf8'))"
        elif has_docker; then
            cat "$file" | docker run --rm -i node:22 node -e "let d=''; process.stdin.on('data', c => d += c); process.stdin.on('end', () => JSON.parse(d));"
        fi
    fi
}

check_no_imperative_ddl() {
    local files=""
    if [ "$MODE" = "staged" ]; then
        files=$(echo "$STAGED_FILES" | grep -E '\.(go|py|ts)$' | grep -v -E '(_test\.go|\.test\.ts|test_.*\.py|/testdata/)' || true)
    else
        files=$(find . -maxdepth 4 -type f \( -name "*.go" -o -name "*.py" -o -name "*.ts" \) \
            ! -name "*_test.go" ! -name "*.test.ts" ! -name "test_*.py" \
            ! -path "*/vendor/*" ! -path "*/node_modules/*" ! -path "*/.git/*" ! -path "*/scripts/*" 2>/dev/null || true)
    fi

    [ -z "$files" ] && return 0

    local violations=""
    violations=$(echo "$files" | while read -r f; do
        [ -f "$f" ] || continue
        # Ignore comments and check for raw DDL
        grep -H -n -i -E '\b(CREATE|ALTER|DROP)[[:space:]]+TABLE\b' "$f" 2>/dev/null | grep -v -E '^[[:space:]]*(//|#|\*)' || true
    done)

    if [ -n "$violations" ]; then
        echo "🚨 [Aerial Verify] Imperative DDL (CREATE/ALTER/DROP TABLE) detected in application code:" >&2
        echo "$violations" | sed 's/^/   • /' >&2
        echo "   Database schema changes must be declared in schema.sql and generated via scripts/atlas-diff.sh." >&2
        return 1
    fi
    return 0
}

CODE_HEALTH_BIN=""

ensure_code_health() {
    local gopath bin_dir src_sum
    gopath="$(go env GOPATH 2>/dev/null || true)"
    bin_dir="${gopath:-$HOME/go}/bin"
    mkdir -p "$bin_dir"
    CODE_HEALTH_BIN="$bin_dir/code-health"
    src_sum=$(cksum "$REPO_ROOT/scripts/code-health/"*.go 2>/dev/null || true)
    if [ ! -x "$CODE_HEALTH_BIN" ] || [ -z "$src_sum" ] || [ "$(cat "${CODE_HEALTH_BIN}.cksum" 2>/dev/null || true)" != "$src_sum" ]; then
        (cd "$REPO_ROOT/scripts/code-health" && go build -o "$CODE_HEALTH_BIN" .)
        printf '%s\n' "$src_sum" > "${CODE_HEALTH_BIN}.cksum"
    fi
}

check_code_health() {
    if has_cmd go && [ -d "$REPO_ROOT/scripts/code-health" ]; then
        ensure_code_health
        if [ "$MODE" = "staged" ]; then
            local go_staged
            go_staged=$(echo "$STAGED_FILES" | grep -E '\.go$|\.code-health-baseline\.json$' || true)
            if [ -n "$go_staged" ]; then
                "$CODE_HEALTH_BIN" -root "$REPO_ROOT" -baseline "$REPO_ROOT/.code-health-baseline.json" >/dev/null
            fi
        else
            echo "   [code-health] Checking ratcheted complexity and length limits..."
            "$CODE_HEALTH_BIN" -root "$REPO_ROOT" -baseline "$REPO_ROOT/.code-health-baseline.json"
        fi
    fi
}

run_atlas_validate() {
    has_cmd atlas || return 0
    find . -maxdepth 3 -name "atlas.hcl" 2>/dev/null | sort | while IFS= read -r hcl; do
        [ -f "$hcl" ] || continue
        local target_dir
        target_dir="$(dirname "$hcl")"
        local mig_dir
        mig_dir=$(grep -E 'dir\s*=' "$hcl" 2>/dev/null | head -n1 | sed -E 's/.*dir\s*=\s*"([^"]+)".*/\1/' || true)
        if [ -z "$mig_dir" ]; then
            mig_dir="file://migrations"
        elif [ "${mig_dir#file://}" = "$mig_dir" ]; then
            mig_dir="file://${mig_dir}"
        fi
        echo "   [atlas validate] Validating migration integrity in ${target_dir} (${mig_dir})..."
        (cd "$target_dir" && atlas migrate validate --dir "$mig_dir")
    done
}

if [ "$MODE" = "staged" ]; then
    if has_cmd git; then
        git rm --cached -q PR_DESCRIPTION.md .pr_description.md 2>/dev/null || true
    fi
    # Fast path: check only services that have staged changes
    STAGED_FILES=$(git diff --cached --name-only 2>/dev/null || true)
    if [ -z "$STAGED_FILES" ]; then
        echo "✅ [Aerial Verify] No staged files to verify."
        exit 0
    fi

    check_utf8_bom
    check_gofmt
    check_no_main_in_tests
    check_rule_file_sizes
    check_no_imperative_ddl
    check_code_health

    if echo "$STAGED_FILES" | grep -q -E '^(\.golangci\.yml|scripts/verify\.sh)$'; then
        for svc in $GO_SERVICES; do
            run_golangci_lint "$svc"
        done
    fi

    # Check Go microservices
    for svc in $GO_SERVICES; do
        if echo "$STAGED_FILES" | grep -q "^$svc/"; then
            pkgs=$(echo "$STAGED_FILES" | grep "^$svc/.*\.go$" | sed "s|^$svc/||" | while read -r f; do dirname "$f"; done | sort -u)
            if [ -n "$pkgs" ]; then
                for p in $pkgs; do
                    if [ ! -d "$svc/$p" ]; then
                        continue
                    fi
                    if [ "$p" = "." ]; then
                        run_golangci_lint "$svc" "."
                        run_go_pkg_coverage "$svc" "."
                    else
                        run_golangci_lint "$svc" "./$p"
                        run_go_pkg_coverage "$svc" "./$p"
                    fi
                done
                run_deadcode "$svc"
            fi
        fi
    done

    # Check Atlas migrations
    if echo "$STAGED_FILES" | grep -q -E "migrations/|atlas\.hcl|schema\.sql"; then
        run_atlas_validate
    fi

    # Check dashboard JS syntax
    if echo "$STAGED_FILES" | grep -q -E "^(services/)?dashboard/"; then
        run_node_syntax "services/dashboard/static/app.js"
    fi

    # Check docs service JS syntax
    if echo "$STAGED_FILES" | grep -q -E "^(services/)?docs-service/"; then
        run_node_syntax "services/docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"
    fi

    # Check homepage JS syntax & unit tests
    if echo "$STAGED_FILES" | grep -q -E "^(services/)?homepage/"; then
        run_node_syntax "services/homepage/lib/merge.js"
        run_node_syntax "services/homepage/prepare-config.js"
        if [ -d "services/homepage/test" ]; then
            run_node_test "services/homepage" "test/*.test.js"
        fi
    fi

    # Check grafana dashboard JSON syntax
    if echo "$STAGED_FILES" | grep -q -E "^(nomad/)?grafana/dashboards/"; then
        for jf in nomad/grafana/dashboards/*.json; do
            run_json_syntax "$jf"
        done
    fi

    if has_cmd git && git diff --quiet 2>/dev/null; then
        git_dir=$(git rev-parse --git-dir 2>/dev/null || true)
        tree_sha=$(git write-tree 2>/dev/null || true)
        if [ -n "$git_dir" ] && [ -n "$tree_sha" ]; then
            printf '%s\n' "$tree_sha" > "$git_dir/.aerial-verified-tree" 2>/dev/null || true
        fi
    fi

    echo "✅ [Aerial Verify] Fast pre-commit checks passed cleanly."
    exit 0
fi

# Full verification: Run entire CI-equivalent verification suite
echo "=== 0. Encoding & BOM Hygiene ==="
check_utf8_bom
check_gofmt
check_no_main_in_tests
check_rule_file_sizes

echo "=== 1. Static Analysis & Linting ==="
for svc in $GO_SERVICES; do
    run_golangci_lint "$svc"
    run_deadcode "$svc"
done
check_no_imperative_ddl
check_code_health
run_atlas_validate

echo "=== 2. Frontend & Script Syntax Checks ==="
run_node_syntax "services/dashboard/static/app.js"
run_node_syntax "services/docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"
run_node_syntax "services/homepage/lib/merge.js"
run_node_syntax "services/homepage/prepare-config.js"
for jf in nomad/grafana/dashboards/*.json; do
    run_json_syntax "$jf"
done

echo "=== 3. Unit Test Suites ==="
for svc in $GO_SERVICES; do
    run_go_test "$svc"
done

if [ -f "services/dashboard/app.test.js" ]; then
    run_node_test "services/dashboard" "*.test.js"
fi

if [ -d "services/homepage/test" ]; then
    run_node_test "services/homepage" "test/*.test.js"
fi

echo "=== 4. Test Coverage Gating ==="
if [ -f "$SCRIPT_DIR/check-coverage.sh" ]; then
    sh "$SCRIPT_DIR/check-coverage.sh" --check
fi

echo "✅ [Aerial Verify] All unit tests, linters, coverage gates, and syntax checks passed with 100% success!"
exit 0
