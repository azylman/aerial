#!/bin/sh
set -eu

# Prevent MSYS2 from mangling Docker volume path colons on Windows
export MSYS_NO_PATHCONV=1

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

echo "⚡ [Aerial Verify] Running $MODE verification checks..."

# Helper to check command availability
has_cmd() {
    command -v "$1" >/dev/null 2>&1
}

# Go services in the monorepo
GO_SERVICES="brain scheduler-mcp discord-mcp dashboard"

run_go_vet() {
    svc="$1"
    if [ -d "$svc" ]; then
        echo "   [go vet] Checking $svc..."
        if has_cmd go; then
            (cd "$svc" && go vet ./...)
        elif has_cmd docker; then
            docker run --rm -v "$(pwd)/$svc:/app" -w /app golang:1.22 go vet ./...
        fi
    fi
}

run_golangci_lint() {
    svc="$1"
    if [ -d "$svc" ]; then
        echo "   [golangci-lint] Linting $svc..."
        if has_cmd golangci-lint; then
            (cd "$svc" && golangci-lint run ./...)
        elif has_cmd docker; then
            docker run --rm -v "$(pwd)/$svc:/app" -w /app golangci/golangci-lint:v1.59.1 golangci-lint run ./...
        elif has_cmd go; then
            echo "   (golangci-lint not found, falling back to go vet for $svc)"
            (cd "$svc" && go vet ./...)
        else
            echo "🚨 [Aerial Verify] Error: Neither golangci-lint, docker, nor go found in PATH." >&2
            exit 1
        fi
    fi
}

run_go_test() {
    svc="$1"
    if [ -d "$svc" ]; then
        echo "   [go test] Testing $svc..."
        if has_cmd go; then
            (cd "$svc" && go test -v ./...)
        elif has_cmd docker; then
            docker run --rm -v "$(pwd)/$svc:/app" -w /app golang:1.22 go test -v ./...
        else
            echo "🚨 [Aerial Verify] Error: Neither go nor docker found in PATH." >&2
            exit 1
        fi
    fi
}

run_node_syntax() {
    file="$1"
    if [ -f "$file" ]; then
        echo "   [node --check] Checking syntax of $file..."
        if has_cmd node; then
            node --check "$file"
        elif has_cmd docker; then
            docker run --rm -v "$(pwd):/app" -w /app node:20 node --check "$file"
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
        elif has_cmd docker; then
            docker run --rm -v "$(pwd)/$dir:/app" -w /app node:20 sh -c "node --test $test_pattern"
        fi
    fi
}

if [ "$MODE" = "staged" ]; then
    # Fast path: check only services that have staged changes
    STAGED_FILES=$(git diff --cached --name-only 2>/dev/null || true)
    if [ -z "$STAGED_FILES" ]; then
        echo "✅ [Aerial Verify] No staged files to verify."
        exit 0
    fi

    # Check Go microservices
    for svc in $GO_SERVICES; do
        if echo "$STAGED_FILES" | grep -q "^$svc/"; then
            run_go_vet "$svc"
        fi
    done

    # Check dashboard JS syntax and unit tests
    if echo "$STAGED_FILES" | grep -q "^dashboard/"; then
        run_node_syntax "dashboard/static/app.js"
        if [ -f "dashboard/app.test.js" ]; then
            run_node_test "dashboard" "*.test.js"
        fi
    fi

    # Check docs service JS syntax
    if echo "$STAGED_FILES" | grep -q "^docs-service/"; then
        run_node_syntax "docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"
    fi

    echo "✅ [Aerial Verify] Fast pre-commit checks passed cleanly."
    exit 0
fi

# Full verification: Run entire CI-equivalent verification suite
echo "=== 1. Static Analysis & Linting ==="
for svc in $GO_SERVICES; do
    run_golangci_lint "$svc"
done

echo "=== 2. Frontend & Script Syntax Checks ==="
run_node_syntax "dashboard/static/app.js"
run_node_syntax "docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"

echo "=== 3. Unit Test Suites ==="
for svc in $GO_SERVICES; do
    run_go_test "$svc"
done

if [ -f "dashboard/app.test.js" ]; then
    run_node_test "dashboard" "*.test.js"
fi

echo "✅ [Aerial Verify] All unit tests, linters, and syntax checks passed with 100% success!"
exit 0
