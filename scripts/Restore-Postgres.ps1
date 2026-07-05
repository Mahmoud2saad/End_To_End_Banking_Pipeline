<#
.SYNOPSIS
    Restores a Postgres backup produced by Backup-Postgres.ps1 into the
    running Airflow metadata container.

.DESCRIPTION
    By default restores into a NEW database (a drill) so the live Airflow
    metadata DB is never touched accidentally. Pass -Force -TargetDb airflow
    to actually replace the live database (stop Airflow first — see
    docs/DR_RUNBOOK.md).

.PARAMETER BackupFile
    Path to the .sql.gz backup file to restore.

.PARAMETER TargetDb
    Database name to restore into. Defaults to a throwaway drill DB name.

.PARAMETER Force
    Required if -TargetDb is the live "airflow" database.

.EXAMPLE
    # Drill — safe, restores into a new throwaway database
    .\Restore-Postgres.ps1 -BackupFile ".\backups\postgres\airflow_meta_20260701T020000Z.sql.gz"

.EXAMPLE
    # Real restore — stop Airflow first: docker compose stop airflow-webserver airflow-scheduler
    .\Restore-Postgres.ps1 -BackupFile ".\backups\postgres\airflow_meta_20260701T020000Z.sql.gz" `
        -TargetDb airflow -Force
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$BackupFile,

    [string]$TargetDb = "airflow_restore_drill",

    [string]$ComposeProjectDir = ".",
    [string]$PgUser = $(if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "airflow" }),

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

if ($TargetDb -eq "airflow" -and -not $Force) {
    Write-Log "ERROR: restoring into the live 'airflow' database requires -Force."
    Write-Log "Stop Airflow first: docker compose stop airflow-webserver airflow-scheduler"
    Write-Log "For a safe drill, omit -TargetDb to restore into a throwaway database instead."
    exit 1
}

# Verify checksum if present
$sidecar = "$BackupFile.sha256"
if (Test-Path $sidecar) {
    Write-Log "Verifying backup checksum..."
    $line = Get-Content $sidecar -Raw
    $expectedHash = ($line -split "\s+")[0].Trim().ToUpper()
    $actualHash = (Get-FileHash -Path $BackupFile -Algorithm SHA256).Hash.ToUpper()
    if ($expectedHash -ne $actualHash) {
        Write-Log "ERROR: checksum verification FAILED — backup file may be corrupt. Aborting."
        exit 1
    }
    Write-Log "Checksum OK."
} else {
    Write-Log "WARNING: no .sha256 sidecar found — skipping integrity check."
}

Push-Location $ComposeProjectDir
try {
    $RawDumpFile = Join-Path $env:TEMP "restore_$([guid]::NewGuid().ToString('N')).dump"

    Write-Log "Decompressing backup..."
    $inStream = [System.IO.File]::OpenRead((Resolve-Path $BackupFile))
    $gzipStream = New-Object System.IO.Compression.GZipStream($inStream, [System.IO.Compression.CompressionMode]::Decompress)
    $outStream = [System.IO.File]::Create($RawDumpFile)
    $gzipStream.CopyTo($outStream)
    $outStream.Close()
    $gzipStream.Close()
    $inStream.Close()

    Write-Log "Creating target database '$TargetDb' (dropping first if it already exists, for a clean restore)..."
    docker compose exec -T postgres dropdb -U $PgUser --if-exists $TargetDb
    docker compose exec -T postgres createdb -U $PgUser $TargetDb

    $containerTempFile = "/tmp/$(Split-Path $RawDumpFile -Leaf)"
    docker compose cp $RawDumpFile "postgres:$containerTempFile"

    Write-Log "Restoring into '$TargetDb'..."
    docker compose exec -T postgres pg_restore -U $PgUser -d $TargetDb $containerTempFile
    docker compose exec -T postgres rm -f $containerTempFile
    Remove-Item $RawDumpFile -Force

    Write-Log "Verifying restore — checking dag_run table row count..."
    $result = docker compose exec -T postgres psql -U $PgUser -d $TargetDb -t -c "SELECT COUNT(*) FROM dag_run;"
    Write-Log "  dag_run rows in restored DB: $($result.Trim())"

    Write-Log "Restore complete into database '$TargetDb'."
    if ($TargetDb -ne "airflow") {
        Write-Log "This was a drill. To promote: stop Airflow, then re-run with -TargetDb airflow -Force."
    } else {
        Write-Log "Restart Airflow now: docker compose start airflow-webserver airflow-scheduler"
    }
    Write-Log "Record this drill in docs/DR_RUNBOOK.md's restore log."
}
finally {
    Pop-Location
}
