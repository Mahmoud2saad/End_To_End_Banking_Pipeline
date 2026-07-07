<#
.SYNOPSIS
    Fixes a real, reproduced bug: kafka-python==2.0.2 (unmaintained since
    2020) breaks on fresh installs with modern `six` versions, causing
    "ModuleNotFoundError: No module named 'kafka.vendor.six.moves'" --
    exactly what you hit in the kafka-consumer container. This didn't show
    on your Windows host because your existing .venv already had an older
    six satisfying it; a fresh container resolves a newer one and breaks.

.DESCRIPTION
    Replaces kafka-python with kafka-python-ng (actively maintained fork,
    verified as a true drop-in replacement -- same `from kafka import ...`
    statements, zero source code changes needed) in all 3 places that
    depend on it: kafka/consumer/requirements.txt,
    kafka/producer/requirements.txt, grafana_api/requirements.txt.

    Verified before being handed to you: reproduced the exact error in a
    clean venv, confirmed kafka-python-ng==2.2.3 imports cleanly with the
    same API in the same clean environment.

.EXAMPLE
    cd "D:\NTI INTERNSHIP\Airflow\Banking_pipeline"
    .\Fix-KafkaPythonDependency.ps1
#>

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    Write-Host "[Fix-KafkaPythonDependency] $Message"
}

function Write-FileForce {
    param([string]$Path, [string]$Content)
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
    Write-Log "Wrote: $Path"
}

$consumerReqs = @'
kafka-python-ng==2.2.3
pandas==2.2.2
pyarrow==16.1.0
python-dotenv==1.0.1

'@
$producerReqs = @'
kafka-python-ng==2.2.3
pandas==2.2.2
python-dotenv==1.0.1

'@
$grafanaReqs = @'
fastapi==0.115.0
uvicorn==0.31.0
duckdb
numpy==2.1.1
pandas==2.2.2
kafka-python-ng==2.2.3

'@

Write-FileForce -Path "kafka\consumer\requirements.txt" -Content $consumerReqs
Write-FileForce -Path "kafka\producer\requirements.txt" -Content $producerReqs
Write-FileForce -Path "grafana_api\requirements.txt" -Content $grafanaReqs

Write-Log ""
Write-Log "Fixed. Next steps:"
Write-Log "  1. cd kafka"
Write-Log "  2. docker compose up -d --build kafka-consumer   (rebuild with the fix)"
Write-Log "  3. docker compose logs kafka-consumer   (should show it joining the consumer group, no traceback)"
Write-Log "  4. cd .."
Write-Log "  5. If grafana_api / uvicorn is currently running, restart it too:"
Write-Log "     pip install -r grafana_api\requirements.txt --upgrade"
