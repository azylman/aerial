# scripts/test-linux.ps1 - Fast Local Linux Verification & Cross-Platform Suite Runner
[CmdletBinding()]
param (
    [string]$Service = "",
    [switch]$Check
)

$ErrorActionPreference = "Stop"

Write-Host "⚡ [Aerial Test Linux] Running Linux verification on Windows..." -ForegroundColor Cyan

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$goServices = if ($Service) { @($Service) } else { @("brain", "scheduler-mcp", "discord-mcp", "dashboard", "sidecars/hangar") }

# 1. Fast Docker Status Probe (2-second ceiling)
$hasDocker = $false
if (Get-Command "docker" -ErrorAction SilentlyContinue) {
    try {
        $dockerCheck = Start-Process -FilePath "docker" -ArgumentList "version --format '{{.Server.Version}}'" -NoNewWindow -PassThru -RedirectStandardOutput "$env:TEMP\aerial-docker-out.tmp" -RedirectStandardError "$env:TEMP\aerial-docker-err.tmp"
        if ($dockerCheck.WaitForExit(2000) -and $dockerCheck.ExitCode -eq 0) {
            $hasDocker = $true
        } else {
            if (-not $dockerCheck.HasExited) {
                $dockerCheck.Kill()
            }
        }
    } catch {
        $hasDocker = $false
    } finally {
        Remove-Item "$env:TEMP\aerial-docker-out.tmp" -Force -ErrorAction SilentlyContinue
        Remove-Item "$env:TEMP\aerial-docker-err.tmp" -Force -ErrorAction SilentlyContinue
    }
}

# 2. Check WSL Availability with accessible /mnt/c
$hasWsl = $false
if (-not $hasDocker -and (Get-Command "wsl" -ErrorAction SilentlyContinue)) {
    try {
        & wsl test -d /mnt/c *>$null
        if ($LASTEXITCODE -eq 0) {
            $hasWsl = $true
        }
    } catch {
        $hasWsl = $false
    }
}

# Branch A: Active Docker Daemon -> Full Linux Container Coverage Execution
if ($hasDocker) {
    Write-Host "   [Docker] Detected active Docker daemon. Running Linux coverage in golang:1.24..." -ForegroundColor DarkCyan
    $svcArg = if ($Service) { "--service $Service" } else { "" }
    $checkArg = if ($Check) { "--check" } else { "" }
    docker run --rm -v "${repoRoot}:/app" -w /app golang:1.24 sh -c "sh scripts/check-coverage.sh $svcArg $checkArg"
    if ($LASTEXITCODE -ne 0) {
        throw "Linux test coverage check failed inside Docker container"
    }
    Write-Host "✅ [Aerial Test Linux] Linux container verification passed with 100% success!" -ForegroundColor Green
    exit 0
}

# Branch B: Active WSL -> Run check-coverage.sh via WSL
if ($hasWsl) {
    Write-Host "   [WSL] Detected active WSL environment. Running Linux coverage via WSL..." -ForegroundColor DarkCyan
    $wslRepoRoot = "/mnt/" + $repoRoot.Substring(0, 1).ToLower() + "/" + $repoRoot.Substring(3).Replace('\', '/')
    $svcArg = if ($Service) { "--service $Service" } else { "" }
    $checkArg = if ($Check) { "--check" } else { "" }
    & wsl sh -c "cd '$wslRepoRoot' && sh scripts/check-coverage.sh $svcArg $checkArg"
    if ($LASTEXITCODE -ne 0) {
        throw "Linux test coverage check failed inside WSL"
    }
    Write-Host "✅ [Aerial Test Linux] Linux WSL verification passed with 100% success!" -ForegroundColor Green
    exit 0
}

# Branch C: Host-Native Go Cross-Compilation Fallback (Zero Docker/WSL dependency)
Write-Host "   [Host-Native] Docker/WSL daemon not running. Executing host-native GOOS=linux compilation checks..." -ForegroundColor Yellow

$hasGo = [bool](Get-Command "go" -ErrorAction SilentlyContinue)
if (-not $hasGo) {
    throw "Go compiler not found in PATH."
}

foreach ($svc in $goServices) {
    $svcPath = Join-Path $repoRoot $svc
    if (-not (Test-Path $svcPath)) {
        continue
    }
    Write-Host "   [GOOS=linux] Compiling test packages for $svc..." -ForegroundColor DarkCyan
    Push-Location $svcPath
    $prevGOOS = $env:GOOS
    $prevGOARCH = $env:GOARCH
    try {
        $env:GOOS = "linux"
        $env:GOARCH = "amd64"
        & go test -exec "cmd.exe /c exit 0" ./...
        if ($LASTEXITCODE -ne 0) {
            throw "Linux cross-compilation failed on $svc"
        }
    } finally {
        if ($null -ne $prevGOOS) { $env:GOOS = $prevGOOS } else { Remove-Item Env:\GOOS -ErrorAction SilentlyContinue }
        if ($null -ne $prevGOARCH) { $env:GOARCH = $prevGOARCH } else { Remove-Item Env:\GOARCH -ErrorAction SilentlyContinue }
        Pop-Location
    }
}

Write-Host "[Aerial Test Linux] Host-native cross-compilation for GOOS=linux succeeded across all services!" -ForegroundColor Green
Write-Host "   Tip: To execute tests inside a Linux kernel and measure statement coverage locally, launch Docker Desktop." -ForegroundColor Gray
exit 0
