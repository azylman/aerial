# scripts/verify.ps1 - Windows Native Pre-Flight Verification Runner
[CmdletBinding()]
param (
    [switch]$Staged,
    [switch]$Full,
    [switch]$CoverageGaps
)

$ErrorActionPreference = "Stop"
$mode = if ($Staged) { "staged" } else { "full" }

Write-Host "⚡ [Aerial Verify] Running $mode verification checks on Windows..." -ForegroundColor Cyan

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$goServices = @("brain", "scheduler-mcp", "discord-mcp", "dashboard", "sidecars/hangar")

$gitCmd = if (Get-Command "git" -ErrorAction SilentlyContinue) { "git" }
          elseif (Test-Path "$env:LOCALAPPDATA\Programs\MinGit\cmd\git.exe") { 
              $env:PATH = "$env:LOCALAPPDATA\Programs\MinGit\cmd;" + $env:PATH
              "$env:LOCALAPPDATA\Programs\MinGit\cmd\git.exe" 
          }
          elseif (Test-Path "C:\Program Files\Git\cmd\git.exe") { 
              $env:PATH = "C:\Program Files\Git\cmd;" + $env:PATH
              "C:\Program Files\Git\cmd\git.exe" 
          }
          else { "git" }

$hasGo = [bool](Get-Command "go" -ErrorAction SilentlyContinue)
$goBinPath = if ($hasGo) {
    try { Join-Path (& go env GOPATH) "bin" } catch { "" }
} else { "" }
$userGoBin = Join-Path $env:USERPROFILE "go\bin"
$hasLint = if (Get-Command "golangci-lint" -ErrorAction SilentlyContinue) { 
    $true 
} elseif ($goBinPath -and (Test-Path (Join-Path $goBinPath "golangci-lint.exe"))) {
    if (-not ($env:PATH -split ';' -contains $goBinPath)) {
        $env:PATH = "$goBinPath;" + $env:PATH
    }
    $true
} elseif (Test-Path (Join-Path $userGoBin "golangci-lint.exe")) {
    if (-not ($env:PATH -split ';' -contains $userGoBin)) {
        $env:PATH = "$userGoBin;" + $env:PATH
    }
    $true
} else { 
    $false 
}
$hasDeadcode = if (Get-Command "deadcode" -ErrorAction SilentlyContinue) { 
    $true 
} elseif ($goBinPath -and (Test-Path (Join-Path $goBinPath "deadcode.exe"))) {
    if (-not ($env:PATH -split ';' -contains $goBinPath)) {
        $env:PATH = "$goBinPath;" + $env:PATH
    }
    $true
} elseif (Test-Path (Join-Path $userGoBin "deadcode.exe")) {
    if (-not ($env:PATH -split ';' -contains $userGoBin)) {
        $env:PATH = "$userGoBin;" + $env:PATH
    }
    $true
} else { 
    $false 
}
$hasDocker = if (Get-Command "docker" -ErrorAction SilentlyContinue) {
    try { & docker version *>$null; $LASTEXITCODE -eq 0 } catch { $false }
} else { $false }
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
        docker run --rm -v "${svcPath}:/app" -w /app golang:1.24 go vet ./...
        if ($LASTEXITCODE -ne 0) { throw "go vet (docker) failed on $svc" }
    }
}

function Run-GolangCILint($svc, $targetPkg = "./...") {
    Write-Host "   [golangci-lint] Linting $svc ($targetPkg)..." -ForegroundColor DarkCyan
    $svcPath = Join-Path $repoRoot $svc
    $normSvc = "$($svc.Replace('\', '/'))/"
    if ($hasLint) {
        Push-Location $svcPath
        $prevToolchain = $env:GOTOOLCHAIN
        try {
            $env:GOTOOLCHAIN = "go1.24.1"
            & golangci-lint run --path-prefix="$normSvc" --config "$repoRoot/.golangci.yml" $targetPkg
            if ($LASTEXITCODE -ne 0) { throw "golangci-lint failed on $svc ($targetPkg)" }
        } finally {
            if ($null -ne $prevToolchain) {
                $env:GOTOOLCHAIN = $prevToolchain
            } else {
                Remove-Item Env:\GOTOOLCHAIN -ErrorAction SilentlyContinue
            }
            Pop-Location
        }
    } elseif ($hasDocker) {
        docker run --rm -v "${repoRoot}:/workspace" -w "/workspace/$svc" golangci/golangci-lint:v1.64.5 golangci-lint run --path-prefix="$normSvc" --config /workspace/.golangci.yml $targetPkg
        if ($LASTEXITCODE -ne 0) { throw "golangci-lint (docker) failed on $svc" }
    } elseif ($hasGo) {
        Write-Host "   (golangci-lint not found, running go vet for $svc)" -ForegroundColor Yellow
        Run-GoVet $svc
    } else {
        throw "Neither golangci-lint, docker, nor go found in PATH."
    }
}

function Ensure-Deadcode {
    if ($script:hasDeadcode) { return }
    if ($hasGo) {
        Write-Host "   [deadcode] Installing deadcode via go install..." -ForegroundColor Yellow
        & go install golang.org/x/tools/cmd/deadcode@v0.50.0
        if ($goBinPath -and (Test-Path (Join-Path $goBinPath "deadcode.exe"))) {
            if (-not ($env:PATH -split ';' -contains $goBinPath)) {
                $env:PATH = "$goBinPath;" + $env:PATH
            }
            $script:hasDeadcode = $true
            return
        }
        if (Test-Path (Join-Path $userGoBin "deadcode.exe")) {
            if (-not ($env:PATH -split ';' -contains $userGoBin)) {
                $env:PATH = "$userGoBin;" + $env:PATH
            }
            $script:hasDeadcode = $true
            return
        }
    }
    throw "Neither deadcode nor go found in PATH to perform dead code analysis."
}

function Run-Deadcode($svc) {
    Write-Host "   [deadcode] Checking $svc for unreachable code..." -ForegroundColor DarkCyan
    Ensure-Deadcode
    $svcPath = Join-Path $repoRoot $svc
    Push-Location $svcPath
    try {
        $output = & deadcode -test ./... 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "deadcode analysis failed on $svc: $output"
        }
        if ($output) {
            $errLines = ($output | Out-String).Trim()
            if ($errLines) {
                throw "Dead code detected in $svc:`n$errLines"
            }
        }
    } finally {
        Pop-Location
    }
}

function Run-GoLinuxCompileCheck($svc) {
    Write-Host "   [linux-cross-compile] Checking $svc for GOOS=linux..." -ForegroundColor DarkCyan
    $svcPath = Join-Path $repoRoot $svc
    if ($hasGo) {
        Push-Location $svcPath
        $prevGOOS = $env:GOOS
        $prevGOARCH = $env:GOARCH
        try {
            $env:GOOS = "linux"
            $env:GOARCH = "amd64"
            & go test -exec "cmd.exe /c exit 0" ./...
            if ($LASTEXITCODE -ne 0) { throw "Linux cross-compilation failed on $svc" }
        } finally {
            if ($null -ne $prevGOOS) { $env:GOOS = $prevGOOS } else { Remove-Item Env:\GOOS -ErrorAction SilentlyContinue }
            if ($null -ne $prevGOARCH) { $env:GOARCH = $prevGOARCH } else { Remove-Item Env:\GOARCH -ErrorAction SilentlyContinue }
            Pop-Location
        }
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
        docker run --rm -v "${svcPath}:/app" -w /app golang:1.24 go test -v ./...
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

function Check-RuleFileSizes {
    $maxBytes = 23040 # 22.5 KB (leaves 512B buffer for SyncRules frontmatter wrapper)
    foreach ($name in @("GEMINI.md", "AGENTS.md")) {
        $path = Join-Path $repoRoot $name
        if (Test-Path $path) {
            $len = (Get-Item $path).Length
            if ($len -gt $maxBytes) {
                throw "Rule file '$name' ($len bytes) exceeds size limit ($maxBytes bytes / 22.5 KB). Antigravity truncates prompt context when compiled rules exceed 23 KB."
            }
        }
    }
}

if ($Staged) {
    Check-RuleFileSizes
    $stagedFiles = & $gitCmd diff --cached --name-only
    if (-not $stagedFiles) {
        Write-Host "✅ [Aerial Verify] No staged files to verify." -ForegroundColor Green
        exit 0
    }

    foreach ($svc in $goServices) {
        $svcStaged = $stagedFiles | Where-Object { $_ -like "$svc/*.go" }
        if ($svcStaged) {
            # Extract distinct package subdirectories relative to the service
            $pkgs = @()
            foreach ($file in $svcStaged) {
                $relFile = $file.Substring($svc.Length + 1)
                $dir = [System.IO.Path]::GetDirectoryName($relFile).Replace('\', '/')
                $pkg = if ([string]::IsNullOrEmpty($dir)) { "." } else { "./$dir" }
                if ($pkgs -notcontains $pkg) {
                    $pkgs += $pkg
                }
            }
            foreach ($p in $pkgs) {
                Run-GolangCILint $svc $p
            }
            Run-Deadcode $svc
        }
    }

    if ($stagedFiles | Where-Object { $_ -like "dashboard/*" }) {
        Run-NodeCheck "dashboard/static/app.js"
    }

    if ($stagedFiles | Where-Object { $_ -like "docs-service/*" }) {
        Run-NodeCheck "docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js"
    }

    Write-Host "✅ [Aerial Verify] Fast pre-commit checks passed cleanly." -ForegroundColor Green
    exit 0
}

Write-Host "=== 0. Rule File Size Budget ===" -ForegroundColor Yellow
Check-RuleFileSizes

Write-Host "=== 1. Static Analysis & Linting ===" -ForegroundColor Yellow
foreach ($svc in $goServices) {
    Run-GolangCILint $svc
    Run-Deadcode $svc
    Run-GoLinuxCompileCheck $svc
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

Write-Host "=== 4. Test Coverage Gating ===" -ForegroundColor Yellow
$checkCovScript = Join-Path $repoRoot "scripts\check-coverage.sh"
if (Test-Path $checkCovScript) {
    $covArgs = @("--check")
    if ($CoverageGaps) {
        $covArgs += "--gaps"
    }
    if (Get-Command "sh" -ErrorAction SilentlyContinue) {
        & sh $checkCovScript $covArgs
        if ($LASTEXITCODE -ne 0) { throw "Coverage threshold verification failed" }
    } elseif ($hasDocker) {
        docker run --rm -v "${repoRoot}:/app" -w /app golang:1.24 sh scripts/check-coverage.sh $covArgs
        if ($LASTEXITCODE -ne 0) { throw "Coverage threshold verification (docker) failed" }
    }
}

Write-Host "✅ [Aerial Verify] All unit tests, linters, coverage gates, and syntax checks passed with 100% success!" -ForegroundColor Green
exit 0
