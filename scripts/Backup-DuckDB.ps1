<#
.SYNOPSIS
    Backs up the DuckDB warehouse file with a timestamped, checksummed copy.

.DESCRIPTION
    Checkpoints the DuckDB file first (flushes WAL) so the backup is
    consistent, copies it, verifies via SHA256 that the copy is byte-identical,
    then verifies the backup actually opens and is queryable. Prunes backups
    older than RetentionDays.

.PARAMETER DuckDbPath
    Path to the live DuckDB warehouse file. Defaults to $env:DUCKDB_PATH or
    .\local_warehouse\banking.duckdb

.PARAMETER BackupDest
    Root backup folder. Defaults to $env:BACKUP_DEST or .\backups

.PARAMETER RetentionDays
    Days to keep old backups. Defaults to $env:BACKUP_RETENTION_DAYS or 30

.EXAMPLE
    .\Backup-DuckDB.ps1

.EXAMPLE
    .\Backup-DuckDB.ps1 -DuckDbPath "D:\Banking_Pipeline\local_warehouse\banking.duckdb"
#>

[CmdletBinding()]
param(
    [string]$DuckDbPath = $(if ($env:DUCKDB_PATH) { $env:DUCKDB_PATH } else { ".\local_warehouse\banking.duckdb" }),
    [string]$BackupDest = $(if ($env:BACKUP_DEST) { $env:BACKUP_DEST } else { ".\backups" }),
    [int]$RetentionDays = $(if ($env:BACKUP_RETENTION_DAYS) { [int]$env:BACKUP_RETENTION_DAYS } else { 30 })
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Write-Host "[$ts] $Message"
}

$BackupDuckdbDest = Join-Path $BackupDest "duckdb"

if (-not (Test-Path $DuckDbPath)) {
    Write-Log "ERROR: source DuckDB file not found at $DuckDbPath"
    exit 1
}

New-Item -ItemType Directory -Force -Path $BackupDuckdbDest | Out-Null

$Timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$BackupFile = Join-Path $BackupDuckdbDest "banking_$Timestamp.duckdb"

# Checkpoint first — flushes any pending WAL so the copy is consistent
# rather than potentially mid-write. Requires the `duckdb` Python package.
Write-Log "Checkpointing DuckDB before backup..."
$checkpointScript = @"
import sys
import duckdb
con = duckdb.connect(sys.argv[1])
con.execute("CHECKPOINT")
con.close()
"@
$checkpointScript | python - $DuckDbPath
if ($LASTEXITCODE -ne 0) {
    Write-Log "ERROR: checkpoint failed"
    exit 2
}

Write-Log "Copying $DuckDbPath -> $BackupFile"
Copy-Item -Path $DuckDbPath -Destination $BackupFile -Force

# Verify: checksum comparison between source and backup
$SourceHash = (Get-FileHash -Path $DuckDbPath -Algorithm SHA256).Hash
$BackupHash = (Get-FileHash -Path $BackupFile -Algorithm SHA256).Hash

if ($SourceHash -ne $BackupHash) {
    Write-Log "ERROR: checksum mismatch between source and backup — backup may be corrupt"
    Remove-Item -Path $BackupFile -Force
    exit 2
}

# Sidecar checksum file — same format the restore script expects:
# "<hash>  <filename>" (two spaces, filename relative to this file's own dir)
"$BackupHash  $(Split-Path $BackupFile -Leaf)" | Out-File -FilePath "$BackupFile.sha256" -Encoding ascii -NoNewline
Add-Content -Path "$BackupFile.sha256" -Value ""

Write-Log "Verifying backup opens and is queryable..."
$verifyScript = @"
import sys
import duckdb
con = duckdb.connect(sys.argv[1], read_only=True)
tables = con.execute(
    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY table_schema, table_name"
).fetchall()
con.close()
schemas = sorted(set(t[0] for t in tables))
print(f"  backup verified: {len(tables)} table(s) readable across {len(schemas)} schema(s): {schemas}")
"@
$verifyScript | python - $BackupFile
if ($LASTEXITCODE -ne 0) {
    Write-Log "ERROR: backup file failed to open/query — treat this backup as INVALID"
    exit 2
}

Write-Log "Pruning backups older than $RetentionDays days..."
$cutoff = (Get-Date).AddDays(-$RetentionDays)
Get-ChildItem -Path $BackupDuckdbDest -Filter "banking_*.duckdb*" |
    Where-Object { $_.LastWriteTime -lt $cutoff } |
    ForEach-Object {
        Write-Log "  removing old backup: $($_.Name)"
        Remove-Item $_.FullName -Force
    }

Write-Log "DuckDB backup complete: $BackupFile"
