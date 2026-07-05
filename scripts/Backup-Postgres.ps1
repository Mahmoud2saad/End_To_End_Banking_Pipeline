<#
.SYNOPSIS
    Backs up the Airflow metadata Postgres DB running in Docker.

.DESCRIPTION
    Runs pg_dump INSIDE the postgres container (via `docker compose exec`)
    since Postgres itself lives in Docker, not on the Windows host — this
    avoids needing pg_dump.exe installed locally. Streams the dump out,
    gzips it, verifies size and gzip integrity, writes a checksum sidecar,
    prunes old backups.

.PARAMETER ComposeProjectDir
    Path to the folder containing docker-compose.yml. Defaults to current directory.

.PARAMETER BackupDest
    Root backup folder. Defaults to $env:BACKUP_DEST or .\backups

.PARAMETER RetentionDays
    Days to keep old backups. Defaults to $env:BACKUP_RETENTION_DAYS or 30

.EXAMPLE
    .\Backup-Postgres.ps1
#>

[CmdletBinding()]
param(
    [string]$ComposeProjectDir = ".",
    [string]$BackupDest = $(if ($env:BACKUP_DEST) { $env:BACKUP_DEST } else { ".\backups" }),
    [int]$RetentionDays = $(if ($env:BACKUP_RETENTION_DAYS) { [int]$env:BACKUP_RETENTION_DAYS } else { 30 }),
    [string]$PgUser = $(if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "airflow" }),
    [string]$PgDb   = $(if ($env:POSTGRES_DB)   { $env:POSTGRES_DB }   else { "airflow" })
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Write-Host "[$ts] $Message"
}

$BackupPgDest = Join-Path $BackupDest "postgres"
New-Item -ItemType Directory -Force -Path $BackupPgDest | Out-Null

$Timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$BackupFile = Join-Path $BackupPgDest "airflow_meta_$Timestamp.sql.gz"
$RawDumpFile = Join-Path $env:TEMP "airflow_meta_$Timestamp.dump"

Push-Location $ComposeProjectDir
try {
    Write-Log "Dumping Postgres database '$PgDb' from the running container..."

    # pg_dump's custom format (-Fc) is binary — write it inside the container
    # to a temp path, then copy it out, rather than piping through stdout
    # (avoids CRLF/encoding issues from PowerShell's pipeline on binary data).
    $containerTempFile = "/tmp/airflow_meta_$Timestamp.dump"

    docker compose exec -T postgres pg_dump -U $PgUser -d $PgDb --format=custom -f $containerTempFile
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR: pg_dump failed inside the container"
        exit 1
    }

    docker compose cp "postgres:$containerTempFile" $RawDumpFile
    docker compose exec -T postgres rm -f $containerTempFile

    if (-not (Test-Path $RawDumpFile) -or (Get-Item $RawDumpFile).Length -eq 0) {
        Write-Log "ERROR: dump file is missing or empty after copying out of the container"
        exit 2
    }

    Write-Log "Compressing dump..."
    # .NET GZipStream — avoids depending on an external gzip.exe on Windows
    $inStream = [System.IO.File]::OpenRead($RawDumpFile)
    $outStream = [System.IO.File]::Create($BackupFile)
    $gzipStream = New-Object System.IO.Compression.GZipStream($outStream, [System.IO.Compression.CompressionMode]::Compress)
    $inStream.CopyTo($gzipStream)
    $gzipStream.Close()
    $outStream.Close()
    $inStream.Close()

    Remove-Item $RawDumpFile -Force

    if (-not (Test-Path $BackupFile) -or (Get-Item $BackupFile).Length -eq 0) {
        Write-Log "ERROR: compressed backup file is empty"
        exit 2
    }

    $Hash = (Get-FileHash -Path $BackupFile -Algorithm SHA256).Hash
    "$Hash  $(Split-Path $BackupFile -Leaf)" | Out-File -FilePath "$BackupFile.sha256" -Encoding ascii

    Write-Log "Pruning backups older than $RetentionDays days..."
    $cutoff = (Get-Date).AddDays(-$RetentionDays)
    Get-ChildItem -Path $BackupPgDest -Filter "airflow_meta_*.sql.gz*" |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        ForEach-Object {
            Write-Log "  removing old backup: $($_.Name)"
            Remove-Item $_.FullName -Force
        }

    $sizeMB = [math]::Round((Get-Item $BackupFile).Length / 1MB, 2)
    Write-Log "Postgres backup complete: $BackupFile ($sizeMB MB)"
}
finally {
    Pop-Location
}
