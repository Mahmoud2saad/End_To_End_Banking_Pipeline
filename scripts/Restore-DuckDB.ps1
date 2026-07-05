<#
.SYNOPSIS
    Restores a DuckDB backup produced by Backup-DuckDB.ps1.

.DESCRIPTION
    ALWAYS restores to a new/different target by default — refuses to
    overwrite an existing file unless -Force is passed. This means a restore
    drill can never accidentally destroy a working warehouse.

.PARAMETER BackupFile
    Path to the .duckdb backup file to restore.

.PARAMETER Target
    Where to restore to. Defaults to $env:DUCKDB_PATH or .\local_warehouse\banking.duckdb

.PARAMETER Force
    Required to overwrite an existing target. Omit this for a safe drill
    (restore to a throwaway -Target path instead).

.EXAMPLE
    # Drill — safe, restores to a throwaway path
    .\Restore-DuckDB.ps1 -BackupFile ".\backups\duckdb\banking_20260701T023000Z.duckdb" -Target "C:\temp\drill.duckdb"

.EXAMPLE
    # Real restore — overwrites the live warehouse
    .\Restore-DuckDB.ps1 -BackupFile ".\backups\duckdb\banking_20260701T023000Z.duckdb" `
        -Target ".\local_warehouse\banking.duckdb" -Force
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$BackupFile,

    [string]$Target = $(if ($env:DUCKDB_PATH) { $env:DUCKDB_PATH } else { ".\local_warehouse\banking.duckdb" }),

    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Write-Host "[$ts] $Message"
}

if (-not (Test-Path $BackupFile)) {
    Write-Log "ERROR: backup file not found: $BackupFile"
    exit 1
}

# Verify checksum if a .sha256 sidecar exists
$sidecar = "$BackupFile.sha256"
if (Test-Path $sidecar) {
    Write-Log "Verifying backup checksum..."
    $line = Get-Content $sidecar -Raw
    $expectedHash = ($line -split "\s+")[0].Trim().ToUpper()
    $actualHash = (Get-FileHash -Path $BackupFile -Algorithm SHA256).Hash.ToUpper()

    if ($expectedHash -ne $actualHash) {
        Write-Log "ERROR: checksum verification FAILED — backup file may be corrupt. Aborting."
        Write-Log "  expected: $expectedHash"
        Write-Log "  actual:   $actualHash"
        exit 1
    }
    Write-Log "Checksum OK."
} else {
    Write-Log "WARNING: no .sha256 sidecar found — skipping integrity check."
}

if ((Test-Path $Target) -and (-not $Force)) {
    Write-Log "TARGET already exists: $Target"
    Write-Log "Refusing to overwrite without -Force."
    Write-Log "This is intentional — a restore drill should never risk the live warehouse."
    Write-Log "Either pass a different -Target path for a drill, or add -Force for a real restore."
    exit 1
}

$targetDir = Split-Path $Target -Parent
if ($targetDir -and -not (Test-Path $targetDir)) {
    New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
}

Write-Log "Restoring $BackupFile -> $Target"
Copy-Item -Path $BackupFile -Destination $Target -Force

Write-Log "Verifying restored file opens and is queryable..."
$verifyScript = @"
import sys
import duckdb
con = duckdb.connect(sys.argv[1], read_only=True)
tables = con.execute(
    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY table_schema, table_name"
).fetchall()
schemas = sorted(set(t[0] for t in tables))
print(f"  restore verified: {len(tables)} table(s) readable across {len(schemas)} schema(s): {schemas}")
for schema, name in tables[:15]:
    count = con.execute(f'SELECT COUNT(*) FROM "{schema}"."{name}"').fetchone()[0]
    print(f"    {schema}.{name}: {count:,} rows")
if len(tables) > 15:
    print(f"    ... and {len(tables) - 15} more")
con.close()
"@
$verifyScript | python - $Target
if ($LASTEXITCODE -ne 0) {
    Write-Log "ERROR: restored file failed to open/query — restore did NOT succeed cleanly"
    exit 2
}

Write-Log "Restore complete: $Target"
Write-Log "Record this drill in docs/DR_RUNBOOK.md's restore log if this was a scheduled test."
