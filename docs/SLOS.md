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
