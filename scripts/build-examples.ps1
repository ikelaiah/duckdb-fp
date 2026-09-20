#Requires -Version 7.0
<#
.SYNOPSIS
    Compiles every example project and collects the binaries in `example-bin/`.

.DESCRIPTION
    Builds all `examples/*/*.lpi` projects with lazbuild and copies the resulting
    executables, the DuckDB shared library, any example data files, and the
    `sample_data/` folder into a single output directory so the examples can be
    run from one place.

.PARAMETER LazBuild
    Name of, or path to, the lazbuild executable. Defaults to `lazbuild` on PATH.

.PARAMETER OutputDir
    Directory that receives the compiled binaries. Defaults to `<repo>/example-bin`.

.PARAMETER BuildMode
    Optional Lazarus build mode to use (for example `Release`).

.PARAMETER Clean
    Remove the output directory before building.

.EXAMPLE
    pwsh -File scripts/build-examples.ps1

.EXAMPLE
    pwsh -File scripts/build-examples.ps1 -BuildMode Release -Clean
#>
[CmdletBinding()]
param(
    [string]$LazBuild = 'lazbuild',
    [string]$OutputDir,
    [string]$BuildMode,
    [switch]$Clean
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $repoRoot 'example-bin'
}

$lazBuildCommand = Get-Command $LazBuild -ErrorAction SilentlyContinue
if (-not $lazBuildCommand) {
    throw "lazbuild was not found ('$LazBuild'). Install Lazarus/FPC or pass -LazBuild <path-to-lazbuild>."
}
$lazBuildPath = $lazBuildCommand.Source

if ($Clean -and (Test-Path -LiteralPath $OutputDir)) {
    Remove-Item -LiteralPath $OutputDir -Recurse -Force
}
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

$exeSuffix = if ($IsWindows) { '.exe' } else { '' }
$libraryName = if ($IsWindows) { 'duckdb.dll' } elseif ($IsMacOS) { 'libduckdb.dylib' } else { 'libduckdb.so' }
$dataPatterns = @('*.csv', '*.parquet', '*.db', '*.duckdb')

$examplesRoot = Join-Path $repoRoot 'examples'
$projects = @(
    Get-ChildItem -LiteralPath $examplesRoot -Directory |
        ForEach-Object { Get-ChildItem -LiteralPath $_.FullName -Filter '*.lpi' -File } |
        Where-Object { $_.DirectoryName -notmatch '[\\/]backup$' } |
        Sort-Object FullName
)

if ($projects.Count -eq 0) {
    throw "No example projects (*.lpi) found in $examplesRoot"
}

$logLines = [System.Collections.Generic.List[string]]::new()
$failures = [System.Collections.Generic.List[string]]::new()

Write-Host "Building $($projects.Count) example project(s) -> $OutputDir" -ForegroundColor Cyan

foreach ($project in $projects) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($project.Name)
    Write-Host "  Building $($project.FullName)" -ForegroundColor Yellow

    $arguments = @()
    if (-not [string]::IsNullOrWhiteSpace($BuildMode)) {
        $arguments += "--build-mode=$BuildMode"
    }
    $arguments += $project.FullName

    $output = & $lazBuildPath @arguments 2>&1 | Out-String
    $logLines.Add("===== $($project.FullName) =====")
    $logLines.Add($output.TrimEnd())

    if ($LASTEXITCODE -ne 0) {
        Write-Host $output.TrimEnd()
        Write-Host "  FAILED: $name" -ForegroundColor Red
        $failures.Add($name)
        continue
    }

    $executable = Join-Path $project.DirectoryName ($name + $exeSuffix)
    if (-not (Test-Path -LiteralPath $executable)) {
        Write-Host "  FAILED: expected executable not found ($executable)" -ForegroundColor Red
        $failures.Add($name)
        continue
    }

    Copy-Item -LiteralPath $executable -Destination $OutputDir -Force

    foreach ($pattern in $dataPatterns) {
        Get-ChildItem -LiteralPath $project.DirectoryName -Filter $pattern -File |
            ForEach-Object {
                $destination = Join-Path $OutputDir $_.Name
                if (-not (Test-Path -LiteralPath $destination)) {
                    Copy-Item -LiteralPath $_.FullName -Destination $destination -Force
                }
            }
    }

    Write-Host "  OK: $name$exeSuffix" -ForegroundColor Green
}

# The DuckDB shared library must sit next to the executables.
$librarySource = Join-Path (Join-Path $repoRoot 'dll') $libraryName
if (Test-Path -LiteralPath $librarySource) {
    Copy-Item -LiteralPath $librarySource -Destination $OutputDir -Force
    $logLines.Add("Copied $libraryName to $OutputDir")
} else {
    Write-Host "  WARNING: $librarySource not found; examples in $OutputDir may not run." -ForegroundColor DarkYellow
}

# Sample datasets are loaded relative to the executable by DuckDB.SampleData.
$sampleDataSource = Join-Path $repoRoot 'sample_data'
if (Test-Path -LiteralPath $sampleDataSource) {
    $sampleDataDestination = Join-Path $OutputDir 'sample_data'
    New-Item -ItemType Directory -Path $sampleDataDestination -Force | Out-Null
    Copy-Item -Path (Join-Path $sampleDataSource '*') -Destination $sampleDataDestination -Recurse -Force
    $logLines.Add("Copied sample_data to $sampleDataDestination")
}

$logFile = Join-Path $OutputDir 'build.log'
Set-Content -LiteralPath $logFile -Value $logLines

Write-Host ''
if ($failures.Count -gt 0) {
    Write-Host "Build finished with $($failures.Count) failure(s): $($failures -join ', ')" -ForegroundColor Red
    exit 1
}

Write-Host "All $($projects.Count) example project(s) built successfully." -ForegroundColor Green
Write-Host "Binaries are in: $OutputDir"
