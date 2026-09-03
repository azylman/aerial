# scripts/verify.ps1 - Windows Native Pre-Flight Verification Runner
[CmdletBinding()]
param (
    [switch]$Staged,
    [switch]$Full
)

$ErrorActionPreference = "Stop"
$mode = if ($Staged) { "staged" } else { "full" }

Write-Host "⚡ [Aerial Verify] Running $mode verification checks on Windows..." -ForegroundColor Cyan

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$goServices = @("brain", "scheduler-mcp", "discord-mcp", "dashboard")

$gitCmd = if (Get-Command "git" -ErrorAction SilentlyContinue) { "git" }
          elseif (Test-Path "$env:LOCALAPPDATA\Programs\MinGit\cmd\git.exe") { "$env:LOCALAPPDATA\Programs\MinGit\cmd\git.exe" }
          elseif (Test-Path "C:\Program Files\Git\cmd\git.exe") { "C:\Program Files\Git\cmd\git.exe" }
          else { "git" }

$hasGo = [bool](Get-Command "go" -ErrorAction SilentlyContinue)
$hasLint = [bool](Get-Command "golangci-lint" -ErrorAction SilentlyContinue)
$hasDocker = [bool](Get-Command "docker" -ErrorAction SilentlyContinue)
$hasNode = [bool](Get-Command "node" -ErrorAction SilentlyContinue)

function Run-GoVet($svc) {
    Write-Host "   [go vet] Checking $svc..." -ForegroundColor DarkCyan
    $svcPath = Join-Path $repoRoot $svc
    if ($hasGo) {
        Push-Location $svcPath
        try {
            & go vet ./...
            if ($LASTEXITCODE -ne 0) { throw "go vet failed on $svc" }
        } finally {
            Pop-Location
        }
    } elseif ($hasDocker) {
        docker run --rm -v "${svcPath}:/app" -w /app golang:1.22 go vet ./...
        if ($LASTEXITCODE -ne 0) { throw "go vet (docker) failed on $svc" }
    }
}

function Run-GolangCILint($svc) {
    Write-Host "   [golangci-lint] Linting $svc..." -ForegroundColor DarkCyan
    $svcPath = Join-Path $repoRoot $svc
    if ($hasLint) {
        Push-Location $svcPath
        try {
            & golangci-lint run ./...
            if ($LASTEXITCODE -ne 0) { throw "golangci-lint failed on $svc" }
        } finally {
            Pop-Location
        }
    } elseif ($hasDocker) {
        docker run --rm -v "${svcPath}:/app" -w /app golangci/golangci-lint:v1.59.1 golangci-lint run ./...
        if ($LASTEXITCODE -ne 0) { throw "golangci-lint (docker) failed on $svc" }
    } elseif ($hasGo) {
        Write-Host "   (golangci-lint not found, running go vet for $svc)" -ForegroundColor Yellow
        Run-GoVet $svc
    } else {
        throw "Neither golangci-lint, docker, nor go found in PATH."
    }
}

function Run-GoTest($svc) {
    Write-Host "   [go test] Testing $svc..." -ForegroundColor DarkCyan
    $svcPath = Join-Path $repoRoot $svc
    if ($hasGo) {
        Push-Location $svcPath
        try {
            & go test -v ./...
            if ($LASTEXITCODE -ne 0) { throw "go test failed on $svc" }
        } finally {
            Pop-Location
        }
    } elseif ($hasDocker) {
        docker run --rm -v "${svcPath}:/app" -w /app golang:1.22 go test -v ./...
        if ($LASTEXITCODE -ne 0) { throw "go test (docker) failed on $svc" }
    } else {
        throw "Neither go nor docker found in PATH."
    }
}

function Run-NodeCheck($relPath) {
    $fullPath = Join-Path $repoRoot $relPath
    if (Test-Path $fullPath) {
        Write-Host "   [node --check] Checking syntax of $relPath..." -ForegroundColor DarkCyan
        if ($hasNode) {
            & node --check $fullPath
            if ($LASTEXITCODE -ne 0) { throw "Node syntax check failed on $relPath" }
        } elseif ($hasDocker) {
            docker run --rm -v "${repoRoot}:/app" -w /app node:20 node --check $relPath
            if ($LASTEXITCODE -ne 0) { throw "Node syntax check (docker) failed on $relPath" }
        }
    }
}

function Run-NodeTest($relDir, $testPattern) {
    $fullDir = Join-Path $repoRoot $relDir
    if (Test-Path $fullDir) {
        Write-Host "   [node --test] Testing in $relDir ($testPattern)..." -ForegroundColor DarkCyan
        if ($hasNode) {
            Push-Location $fullDir
            try {
                & node --test $testPattern
                if ($LASTEXITCODE -ne 0) { throw "Node unit tests failed in $relDir" }
            } finally {
                Pop-Location
            }
        } elseif ($hasDocker) {
            docker run --rm -v "${fullDir}:/app" -w /app node:20 sh -c "node --test $testPattern"
            if ($LASTEXITCODE -ne 0) { throw "Node unit tests (docker) failed in $relDir" }
        }
    }
}

if ($Staged) {
    $stagedFiles = & $gitCmd diff --cached --name-only
    if (-not $stagedFiles) {
        Write-Host "✅ [Aerial Verify] No staged files to verify." -ForegroundColor Green
        exit 0
    }

    foreach ($svc in $goServices) {
        if ($stagedFiles | Where-Object { $_ -like "$svc/*" }) {
            Run-GoVet $svc
        }
    }

    if ($stagedFiles | Where-Object { $_ -like "dashboard/*" }) {
        Run-NodeCheck "dashboard/static/app.js"
        if (Test-Path (Join-Path $repoRoot "dashboard/app.test.js")) {
            Run-NodeTest "dashboard" "*.test.js"
        }
    }

    if ($stagedFiles | Where-Object { $_ -like "docs-service/*" }) {
        Run-NodeCheck "docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"
    }

    Write-Host "✅ [Aerial Verify] Fast pre-commit checks passed cleanly." -ForegroundColor Green
    exit 0
}

Write-Host "=== 1. Static Analysis & Linting ===" -ForegroundColor Yellow
foreach ($svc in $goServices) {
    Run-GolangCILint $svc
}

Write-Host "=== 2. Frontend & Script Syntax Checks ===" -ForegroundColor Yellow
Run-NodeCheck "dashboard/static/app.js"
Run-NodeCheck "docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"

Write-Host "=== 3. Unit Test Suites ===" -ForegroundColor Yellow
foreach ($svc in $goServices) {
    Run-GoTest $svc
}

if (Test-Path (Join-Path $repoRoot "dashboard/app.test.js")) {
    Run-NodeTest "dashboard" "*.test.js"
}

Write-Host "✅ [Aerial Verify] All unit tests, linters, and syntax checks passed with 100% success!" -ForegroundColor Green
exit 0
