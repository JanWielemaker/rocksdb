#Requires -Version 5.1
<#
.SYNOPSIS
    Install rocksdb pack on Windows with vcpkg integration.

.DESCRIPTION
    Wrapper script that sets up the environment for pack_install to work
    with vcpkg on Windows. Ensures VCPKG_ROOT and MSVC environment are
    properly configured before calling pack_install.

.PARAMETER Interactive
    Run pack_install in interactive mode (prompts for confirmation).
    Default is false (non-interactive).

.EXAMPLE
    .\install-pack-windows.ps1

.EXAMPLE
    .\install-pack-windows.ps1 -Interactive

.NOTES
    Prerequisites:
    - SWI-Prolog installed
    - vcpkg installed at C:\vcpkg (or set VCPKG_ROOT)
    - VS Build Tools 2022 with MSVC and Windows SDK
    - RocksDB installed via vcpkg:
      vcpkg install rocksdb[lz4,snappy,zlib,zstd]:x64-windows
#>

param(
    [switch]$Interactive = $false
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  RocksDB Pack Installer for Windows" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""

# =============================================================================
# Step 1: Check Prerequisites
# =============================================================================

Write-Host "[1/4] Checking prerequisites..." -ForegroundColor Yellow

# Check SWI-Prolog
$swiplPath = "C:\Program Files\swipl\bin\swipl.exe"
if (-not (Test-Path $swiplPath)) {
    Write-Host "ERROR: SWI-Prolog not found at $swiplPath" -ForegroundColor Red
    Write-Host "Please install SWI-Prolog first." -ForegroundColor Red
    exit 1
}
Write-Host "  Found SWI-Prolog: $swiplPath" -ForegroundColor Green

# Check vcpkg
if ($env:VCPKG_ROOT) {
    $vcpkgRoot = $env:VCPKG_ROOT
} elseif (Test-Path "C:\vcpkg") {
    $vcpkgRoot = "C:\vcpkg"
} else {
    Write-Host "ERROR: vcpkg not found" -ForegroundColor Red
    Write-Host "Expected location: C:\vcpkg" -ForegroundColor Red
    Write-Host "Or set VCPKG_ROOT environment variable" -ForegroundColor Red
    exit 1
}

$vcpkgExe = Join-Path $vcpkgRoot "vcpkg.exe"
if (-not (Test-Path $vcpkgExe)) {
    Write-Host "ERROR: vcpkg.exe not found at $vcpkgExe" -ForegroundColor Red
    exit 1
}
Write-Host "  Found vcpkg: $vcpkgRoot" -ForegroundColor Green

# Check RocksDB installation
$rocksdbLib = Join-Path $vcpkgRoot "installed\x64-windows\lib\rocksdb.lib"
if (-not (Test-Path $rocksdbLib)) {
    Write-Host "WARNING: RocksDB not found in vcpkg" -ForegroundColor Yellow
    Write-Host "  Expected: $rocksdbLib" -ForegroundColor Yellow
    Write-Host "  Run: vcpkg install rocksdb[lz4,snappy,zlib,zstd]:x64-windows" -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne 'y') {
        exit 1
    }
} else {
    Write-Host "  Found RocksDB: $rocksdbLib" -ForegroundColor Green
}

# =============================================================================
# Step 2: Set up MSVC Environment
# =============================================================================

Write-Host ""
Write-Host "[2/4] Setting up MSVC environment..." -ForegroundColor Yellow

# Find vcvarsall.bat
$vsSearchPaths = @(
    "C:\Program Files\Microsoft Visual Studio\2022",
    "C:\Program Files (x86)\Microsoft Visual Studio\2022"
)

$vcvarsall = $null
foreach ($basePath in $vsSearchPaths) {
    if (Test-Path $basePath) {
        $vcvarsall = Get-ChildItem -Path $basePath -Recurse -Filter "vcvarsall.bat" -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($vcvarsall) { break }
    }
}

if (-not $vcvarsall) {
    Write-Host "ERROR: vcvarsall.bat not found" -ForegroundColor Red
    Write-Host "Please install VS Build Tools 2022" -ForegroundColor Red
    exit 1
}

Write-Host "  Found vcvarsall: $($vcvarsall.FullName)" -ForegroundColor Green

# Run vcvarsall.bat and capture environment
$tempBat = [System.IO.Path]::GetTempFileName() + ".bat"
$tempEnv = [System.IO.Path]::GetTempFileName()

@"
@echo off
call "$($vcvarsall.FullName)" x64 > nul 2>&1
set > "$tempEnv"
"@ | Out-File -FilePath $tempBat -Encoding ASCII

& cmd.exe /c $tempBat | Out-Null

# Parse and apply environment variables
Get-Content $tempEnv | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        $name = $matches[1]
        $value = $matches[2]
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

Remove-Item $tempBat -ErrorAction SilentlyContinue
Remove-Item $tempEnv -ErrorAction SilentlyContinue

Write-Host "  MSVC environment configured for x64" -ForegroundColor Green

# =============================================================================
# Step 3: Set Environment Variables
# =============================================================================

Write-Host ""
Write-Host "[3/4] Configuring environment..." -ForegroundColor Yellow

$env:VCPKG_ROOT = $vcpkgRoot
Write-Host "  VCPKG_ROOT = $vcpkgRoot" -ForegroundColor Green

$env:VCPKG_DEFAULT_TRIPLET = "x64-windows"
Write-Host "  VCPKG_DEFAULT_TRIPLET = x64-windows" -ForegroundColor Green

# =============================================================================
# Step 4: Run pack_install
# =============================================================================

Write-Host ""
Write-Host "[4/4] Installing rocksdb pack..." -ForegroundColor Yellow
Write-Host ""

$packUrl = "https://github.com/EricGT/rocksdb.git"
$packBranch = "feature/windows-vcpkg-support"

$interactiveFlag = if ($Interactive) { "true" } else { "false" }

Write-Host "Repository: $packUrl" -ForegroundColor Cyan
Write-Host "Branch: $packBranch" -ForegroundColor Cyan
Write-Host "Interactive: $interactiveFlag" -ForegroundColor Cyan
Write-Host ""

$query = "pack_install(rocksdb, [url('$packUrl'), branch('$packBranch'), interactive($interactiveFlag)])"

Write-Host "Running: swipl -g `"$query`" -t halt" -ForegroundColor Cyan
Write-Host ""

& $swiplPath -g $query -t halt

$exitCode = $LASTEXITCODE

Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan

if ($exitCode -eq 0) {
    Write-Host "  Installation SUCCESSFUL" -ForegroundColor Green
    Write-Host "=============================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Test the installation:" -ForegroundColor Yellow
    Write-Host "  swipl -g `"use_module(library(rocksdb)), writeln('rocksdb loaded'), halt`"" -ForegroundColor Gray
} else {
    Write-Host "  Installation FAILED (exit code: $exitCode)" -ForegroundColor Red
    Write-Host "=============================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Check that RocksDB is installed:" -ForegroundColor Gray
    Write-Host "     vcpkg list rocksdb" -ForegroundColor Gray
    Write-Host "  2. If not installed, run:" -ForegroundColor Gray
    Write-Host "     vcpkg install rocksdb[lz4,snappy,zlib,zstd]:x64-windows" -ForegroundColor Gray
    Write-Host "  3. Check build logs in pack directory:" -ForegroundColor Gray
    Write-Host "     %APPDATA%\swi-prolog\pack\rocksdb\build\" -ForegroundColor Gray
    exit $exitCode
}

Write-Host ""
