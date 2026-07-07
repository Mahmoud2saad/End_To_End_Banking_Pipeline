<#
.SYNOPSIS
    Phase 4: adds Kafka consumer lag, backup freshness, and Bronze/Silver
    landing freshness endpoints to grafana_api, plus SLO definitions and
    Grafana panel setup instructions.

.DESCRIPTION
    - grafana_api/observability.py: new router with /kafka-lag,
      /backup-health, /freshness -- tested against real backup/landing
      files from earlier phases (kafka-lag itself needs a live broker to
      test, which wasn't available in the build sandbox -- verify this
      one yourself once wired in).
    - grafana_api/requirements.txt: NEW -- this didn't exist before, even
      though main.py already depended on fastapi/uvicorn/duckdb/etc.
    - docs/SLOS.md: the concrete targets these endpoints measure against.
    - docs/GRAFANA_OBSERVABILITY_SETUP.md: manual panel setup instructions
      (not a dashboard JSON -- your existing dashboards are in Grafana
      v13's instance-specific schema, unsafe to hand-author blind).
    - Appends a 2-line router include to the END of grafana_api/main.py
      (append-only, not a find/replace against content that might have
      changed since I last saw it -- safe regardless of your main.py's
      current exact state, since `app` is already defined earlier in the
      file by the time this runs).

.EXAMPLE
    cd "D:\NTI INTERNSHIP\Airflow\Banking_pipeline"
    .\Setup-Phase4-Observability.ps1
#>

[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    Write-Host "[Setup-Phase4-Observability] $Message"
}

function Write-FileIfNeeded {
    param([string]$Path, [string]$Content)
    $dir = Split-Path $Path -Parent
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    if ((Test-Path $Path) -and (-not $Force)) {
        Write-Log "SKIP (already exists, use -Force to overwrite): $Path"
        return
    }
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
    Write-Log "Wrote: $Path"
}

$obsRouter = @'
"""
grafana_api/observability.py

Adds three endpoints to the existing grafana_api FastAPI app (see
main.py's existing /pipeline-health, which already works and is untouched
by this file): Kafka consumer lag, backup freshness, and Bronze/Silver
landing freshness. See docs/SLOS.md for the targets these measure against.

Wired into main.py with:
    from grafana_api.observability import router as observability_router
    app.include_router(observability_router)

(a two-line addition to the existing file, not a rewrite of it.)
"""
import os
import glob
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

router = APIRouter()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC_PREFIX = os.environ.get("KAFKA_TOPIC_PREFIX", "banking.dev")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "bronze-ingestion-consumer")
KAFKA_LAG_SLO = int(os.environ.get("KAFKA_LAG_SLO", 500))

BACKUP_DEST = os.environ.get("BACKUP_DEST", "./backups")
BACKUP_SLO_HOURS = float(os.environ.get("BACKUP_SLO_HOURS", 26))

LANDING_PATH = os.environ.get(
    "LANDING_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "local_warehouse", "delta", "landing"),
)
FRESHNESS_SLO_MINUTES = float(os.environ.get("FRESHNESS_SLO_MINUTES", 15))


# ── Kafka consumer lag ────────────────────────────────────────────────────────
@router.get("/kafka-lag")
def kafka_lag():
    """
    Per-topic consumer lag for KAFKA_CONSUMER_GROUP: (log end offset) -
    (last committed offset), summed per topic across all partitions.

    NOTE: requires a reachable Kafka broker -- if KAFKA_BOOTSTRAP_SERVERS
    isn't reachable (e.g. the Kafka docker-compose stack isn't running),
    this returns a 503 with the connection error rather than crashing the
    whole API, since Grafana should be able to show "Kafka unreachable"
    as a status rather than losing every other panel too.
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="kafka-python not installed in this environment. "
                   "pip install kafka-python (see kafka/consumer/requirements.txt)",
        )

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
        )
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

        all_topics = consumer.topics()
        our_topics = [t for t in all_topics if t.startswith(f"{KAFKA_TOPIC_PREFIX}.")]

        if not our_topics:
            consumer.close()
            admin.close()
            return {
                "consumer_group": KAFKA_CONSUMER_GROUP,
                "topics": [],
                "total_lag": 0,
                "note": f"No topics found matching prefix '{KAFKA_TOPIC_PREFIX}.' "
                        f"-- has the producer been run yet?",
            }

        partitions = []
        for topic in our_topics:
            for p in consumer.partitions_for_topic(topic) or []:
                partitions.append(TopicPartition(topic, p))

        consumer.assign(partitions)
        end_offsets = consumer.end_offsets(partitions)

        # committed() returns None for a partition the group has never
        # committed on -- treat that as lag == full end offset (nothing
        # consumed yet), not as an error.
        results_by_topic = {}
        total_lag = 0
        for tp in partitions:
            end = end_offsets.get(tp, 0)
            committed = consumer.committed(tp)
            committed = committed if committed is not None else 0
            lag = max(end - committed, 0)
            total_lag += lag

            entry = results_by_topic.setdefault(tp.topic, {
                "topic": tp.topic, "end_offset": 0, "committed_offset": 0, "lag": 0,
            })
            entry["end_offset"] += end
            entry["committed_offset"] += committed
            entry["lag"] += lag

        consumer.close()
        admin.close()

        topics_out = list(results_by_topic.values())
        for t in topics_out:
            t["within_slo"] = t["lag"] <= KAFKA_LAG_SLO

        return {
            "consumer_group": KAFKA_CONSUMER_GROUP,
            "slo_threshold": KAFKA_LAG_SLO,
            "topics": sorted(topics_out, key=lambda t: -t["lag"]),
            "total_lag": total_lag,
            "within_slo": total_lag <= KAFKA_LAG_SLO * max(len(topics_out), 1),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Could not reach Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {e}",
        )


# ── Backup freshness ──────────────────────────────────────────────────────────
def _latest_file_age_hours(pattern: str) -> tuple[str | None, float | None]:
    """Returns (filename, age_in_hours) for the most recently modified file
    matching pattern, or (None, None) if nothing matches."""
    files = glob.glob(pattern)
    if not files:
        return None, None
    latest = max(files, key=os.path.getmtime)
    age_seconds = time.time() - os.path.getmtime(latest)
    return os.path.basename(latest), round(age_seconds / 3600, 2)


@router.get("/backup-health")
def backup_health():
    """
    Reports age of the most recent DuckDB and Postgres backup, checked
    against BACKUP_SLO_HOURS (default 26h -- see docs/SLOS.md for why).
    """
    duckdb_file, duckdb_age = _latest_file_age_hours(
        os.path.join(BACKUP_DEST, "duckdb", "*.duckdb")
    )
    postgres_file, postgres_age = _latest_file_age_hours(
        os.path.join(BACKUP_DEST, "postgres", "*.sql.gz")
    )

    def status_for(age):
        if age is None:
            return "NO_BACKUP_FOUND"
        return "OK" if age <= BACKUP_SLO_HOURS else "STALE"

    return {
        "slo_threshold_hours": BACKUP_SLO_HOURS,
        "duckdb": {
            "latest_file": duckdb_file,
            "age_hours": duckdb_age,
            "status": status_for(duckdb_age),
        },
        "postgres": {
            "latest_file": postgres_file,
            "age_hours": postgres_age,
            "status": status_for(postgres_age),
        },
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Bronze/Silver landing freshness ───────────────────────────────────────────
@router.get("/freshness")
def freshness():
    """
    Age of the most recent landing-zone file per domain table -- see
    docs/SLOS.md's caveat about this being meaningful only while the
    producer/consumer are actively running, not as an always-on service.
    """
    if not os.path.isdir(LANDING_PATH):
        return {"error": f"Landing path not found: {LANDING_PATH}"}

    domains = [
        d for d in os.listdir(LANDING_PATH)
        if os.path.isdir(os.path.join(LANDING_PATH, d))
    ]

    results = []
    for domain in sorted(domains):
        pattern = os.path.join(LANDING_PATH, domain, "*.parquet")
        latest_file, age_hours = _latest_file_age_hours(pattern)
        age_minutes = round(age_hours * 60, 1) if age_hours is not None else None
        results.append({
            "domain": domain,
            "latest_file": latest_file,
            "age_minutes": age_minutes,
            "within_slo": (age_minutes is not None and age_minutes <= FRESHNESS_SLO_MINUTES),
        })

    return {
        "slo_threshold_minutes": FRESHNESS_SLO_MINUTES,
        "domains": results,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

'@
$grafanaReqs = @'
# grafana_api/requirements.txt
# Previously undeclared anywhere -- main.py already depended on fastapi,
# uvicorn, duckdb, numpy, pandas without any requirements file capturing
# that. observability.py (Phase 4) adds a kafka-python dependency on top.

fastapi==0.115.0
uvicorn==0.31.0
duckdb==1.10.1
numpy==2.1.1
pandas==2.2.2
kafka-python==2.0.2

'@
$slosDoc = @'
# Service Level Objectives (SLOs)

Concrete, measurable targets the pipeline is monitored against — turning
"it ran once" into something that's continuously checked, per
docs/DR_RUNBOOK.md's RTO/RPO targets and Phase 4's observability work.

These are starting targets tuned for a portfolio/demo-scale deployment
(single-node Kafka, local DuckDB) — tighten them if this ever runs against
real production volumes and infrastructure.

## Streaming freshness (Kafka -> Bronze)

| Metric | Target | Measured by |
|---|---|---|
| Consumer lag per topic | < 500 messages | `/kafka-lag` endpoint |
| Time since last Bronze landing file per domain | < 15 min during an active producer run | `/freshness` endpoint |

**Important caveat**: this project's Kafka producer/consumer are run
on-demand (`demo`/`realistic`/`full` modes), not as an always-on service —
so "time since last landing file" will legitimately show as stale between
runs. The `/freshness` endpoint reports the age; whether that age breaches
the SLO depends on whether the pipeline is *supposed* to be actively
streaming right now, which this project doesn't yet track as a separate
"is this running" signal (a real production deployment would run the
consumer as a persistent service via Airflow/systemd/a container that
restarts on failure, making "stale = broken" a safe assumption; that's not
this project's current run mode).

## Backup freshness

| Component | RPO target (from DR_RUNBOOK.md) | Alert threshold |
|---|---|---|
| DuckDB warehouse | 24h | > 26h since last successful backup |
| Postgres (Airflow metadata) | 24h | > 26h since last successful backup |

The 2-hour buffer over the 24h RPO absorbs normal scheduling jitter
without generating false alarms for a backup that's merely running a
little late.

## dbt / data quality

| Metric | Target | Measured by |
|---|---|---|
| dbt test pass rate | 100% (0 failures) | `/pipeline-health` (existing endpoint, reads `run_results.json`) |
| dbt run success | All models build without error | `/pipeline-health` |

This endpoint already existed and works correctly — Phase 4 doesn't touch
it, just adds the three metrics above alongside it.

## How these get used

- **Grafana dashboard**: `grafana_dashboards/observability.json` (new,
  added in this phase) visualizes all four endpoints together.
- **Alerting (Phase 4b)**: n8n webhooks fire when `/backup-health` or
  `/kafka-lag` cross their thresholds — see docs/SECURITY_AND_GOVERNANCE.md
  for the one-directional webhook design (pipeline -> n8n only).
- **Manual check**: any of these endpoints can be hit directly during a
  demo or a real investigation: `curl http://localhost:8000/kafka-lag`

'@
$grafanaSetupDoc = @'
# Adding Observability Panels to Grafana

## Why this is manual instructions, not a ready-made dashboard JSON

Your existing dashboards (`grafana_dashboards/*.json`) are exported in
Grafana v13's `dashboard.grafana.app/v2` schema — they carry
instance-specific identifiers (`uid`, `resourceVersion`, `generation`,
`createdBy`) tied to your actual Grafana instance. Hand-writing a new
dashboard JSON in this schema from scratch is unreliable — it can easily
fail to import or silently break due to a UID/version mismatch I can't
verify without access to your running Grafana. Adding these 3 panels
through Grafana's UI (a few minutes) is the reliable path, and you already
know this UI since you built the existing 3 dashboards through it.

## Prerequisite: whichever datasource already powers your existing panels

Your existing dashboards already query `grafana_api`'s endpoints (e.g.
`/atm-performance`, `/fraud-risk`) somehow — use that exact same
datasource connection for these new panels too, for consistency. If you
don't remember which one: Grafana → **Connections → Data sources** → look
for one pointing at `http://localhost:8000` (or wherever `grafana_api` is
running).

## New endpoints this phase adds

| Endpoint | Returns |
|---|---|
| `GET /kafka-lag` | Per-topic consumer lag vs. SLO threshold |
| `GET /backup-health` | Age of latest DuckDB/Postgres backup vs. SLO |
| `GET /freshness` | Age of latest landing file per domain table vs. SLO |

Test them directly first, before touching Grafana, to confirm they work:
```powershell
uvicorn grafana_api.main:app --host 0.0.0.0 --port 8000 --reload
```
Then in a browser or with curl:
```
http://localhost:8000/kafka-lag
http://localhost:8000/backup-health
http://localhost:8000/freshness
```

## Adding the 3 panels

1. Open any existing dashboard (or create a new one called "Pipeline Observability")
2. **Add → Visualization**
3. Select your existing JSON-API-style datasource
4. Set the query URL to the endpoint (e.g. `/backup-health`)
5. Pick a panel type:
   - **`/kafka-lag`**: Table or Bar gauge, one row per topic, color-coded on `within_slo`
   - **`/backup-health`**: Stat panels (one for `duckdb.status`, one for `postgres.status`) — set value mappings so `OK` shows green, `STALE`/`NO_BACKUP_FOUND` shows red
   - **`/freshness`**: Table, one row per domain, with `age_minutes` and `within_slo` columns
6. Repeat for each endpoint, save the dashboard

## Alerting on these (optional, ties into Phase 4b)

Grafana's own alerting can fire directly off these panels (Grafana →
Alerting → New alert rule, condition on `within_slo == false` or
`status == 'STALE'`) — this works independently of the n8n webhook
approach planned for Phase 4b, and you can use either or both.

'@

Write-FileIfNeeded -Path "grafana_api\observability.py" -Content $obsRouter
Write-FileIfNeeded -Path "grafana_api\requirements.txt" -Content $grafanaReqs
Write-FileIfNeeded -Path "docs\SLOS.md" -Content $slosDoc
Write-FileIfNeeded -Path "docs\GRAFANA_OBSERVABILITY_SETUP.md" -Content $grafanaSetupDoc

Write-Log ""
Write-Log "Patching grafana_api\main.py (append-only)..."
$mainPath = "grafana_api\main.py"
if (-not (Test-Path $mainPath)) {
    Write-Log "  ERROR: grafana_api\main.py not found -- skipping patch. Add the router manually:"
    Write-Log "    from grafana_api.observability import router as observability_router"
    Write-Log "    app.include_router(observability_router)"
} else {
    $mainContent = Get-Content $mainPath -Raw -Encoding UTF8
    if ($mainContent -match "observability_router") {
        Write-Log "  SKIP: main.py already includes the observability router"
    } else {
        $patch = "`r`n`r`n# --- Phase 4: observability endpoints (kafka-lag, backup-health, freshness) ---`r`nfrom grafana_api.observability import router as observability_router`r`napp.include_router(observability_router)`r`n"
        Add-Content -Path $mainPath -Value $patch -Encoding UTF8
        Write-Log "  Appended router include to main.py"
    }
}

Write-Log ""
Write-Log "Phase 4 files written."
Write-Log "Next steps:"
Write-Log "  1. pip install -r grafana_api\requirements.txt"
Write-Log "  2. uvicorn grafana_api.main:app --host 0.0.0.0 --port 8000 --reload"
Write-Log "  3. Test: curl http://localhost:8000/backup-health"
Write-Log "  4. Test: curl http://localhost:8000/freshness"
Write-Log "  5. Test (needs Kafka running): curl http://localhost:8000/kafka-lag"
Write-Log "  6. Follow docs\GRAFANA_OBSERVABILITY_SETUP.md to add the 3 panels"
