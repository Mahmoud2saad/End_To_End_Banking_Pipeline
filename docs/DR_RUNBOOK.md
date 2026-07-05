# Disaster Recovery Runbook

"We have backups" only counts once a restore has actually been run and
verified. This document defines targets, the backup schedule, exact
restore steps per component, and a log of drills actually performed —
not just a claim that the scripts exist.

## RTO / RPO Targets

| Component | RPO (max data loss) | RTO (max time to restore) |
|---|---|---|
| Postgres (Airflow metadata) | 24h (nightly backup) | 30 min |
| DuckDB warehouse (Gold/marts) | 24h (nightly backup) | 15 min |
| Delta tables (Bronze/Silver) | Governed by Kafka retention (`KAFKA_RETENTION_HOURS`, default 7 days) — see below | 2–4h (reprocess from Kafka offset) |
| Kafka topics themselves | N/A — Kafka is the durable log; source-of-truth replay window = retention period | N/A |

These are starting targets, not fixed — tune them against actual business
requirements (e.g. a fraud team may need a tighter RPO than a BI analyst).

## Backup Schedule

| Job | Schedule | Script | Destination |
|---|---|---|---|
| Postgres dump | Nightly 02:00 UTC | `scripts/backup_postgres.sh` | `$BACKUP_DEST/postgres/` |
| DuckDB checkpoint + copy | Nightly 02:30 UTC (after Postgres) | `scripts/backup_duckdb.sh` | `$BACKUP_DEST/duckdb/` |
| Delta table `VACUUM` | Weekly, Sunday 03:00 UTC | (Phase 1 dbt macro — retains 7 days of time-travel history per Delta's default) | in-place, not a separate backup |

Retention: `BACKUP_RETENTION_DAYS` (default 30) for Postgres/DuckDB
backups. In staging/prod, `BACKUP_DEST` should point at versioned object
storage (Azure Blob with soft-delete, or S3 with versioning) rather than a
local disk — a local-only backup doesn't protect against the machine
itself failing.

## Restore Procedures

### DuckDB warehouse

```bash
# 1. ALWAYS drill first — restore to a throwaway path, never the live file directly
./scripts/restore_duckdb.sh backups/duckdb/banking_<timestamp>.duckdb --target /tmp/drill.duckdb

# 2. Confirm row counts / spot-check tables look right (the script prints this automatically)

# 3. Only once confirmed, do the real restore
./scripts/restore_duckdb.sh backups/duckdb/banking_<timestamp>.duckdb \
    --target ./local_warehouse/banking.duckdb --force

# 4. Re-run dbt to confirm the restored warehouse is consistent with current models
cd banking_dbt && dbt test
```

The script refuses to overwrite an existing target without `--force` —
this is intentional so a restore drill can never accidentally destroy a
working system. (This safety check was itself verified working during
Phase -1 testing — see drill log below.)

### Postgres (Airflow metadata)

```bash
# 1. Stop the Airflow webserver/scheduler to avoid writes during restore
docker compose stop airflow-webserver airflow-scheduler

# 2. Restore into a NEW database first for a drill:
gunzip -c backups/postgres/airflow_meta_<timestamp>.sql.gz | \
    pg_restore -h localhost -U airflow -d airflow_restore_drill --create

# 3. Spot-check: DAG run history present, connections present
psql -h localhost -U airflow -d airflow_restore_drill -c "SELECT COUNT(*) FROM dag_run;"

# 4. Real restore (only after drill confirms integrity):
gunzip -c backups/postgres/airflow_meta_<timestamp>.sql.gz | \
    pg_restore -h localhost -U airflow -d airflow --clean --if-exists

# 5. Restart Airflow
docker compose start airflow-webserver airflow-scheduler
```

### Bronze/Silver Delta tables — via Kafka replay (Phase 3)

Delta tables are not separately backed up; recovery is by replaying Kafka
from a retained offset:

```bash
# 1. Identify the last known-good offset per topic (from consumer group metrics / audit log)
# 2. Reset the consumer group to that offset
kafka-consumer-groups --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --group bronze-ingestion-consumer --reset-offsets --to-offset <offset> \
    --topic banking.prod.card_transactions --execute

# 3. Restart the consumer — it will reprocess from that point, rebuilding Bronze
```

This is why `KAFKA_RETENTION_HOURS` directly defines your Bronze RPO: you
cannot recover data older than the retention window, since it's no longer
in the log. If your compliance requirement exceeds the practical retention
window, that data needs a separate cold-storage export — not something to
discover during an actual incident.

## Restore Drill Log

Every restore drill actually performed gets logged here — this is what
turns "we have backups" into "we know the backups work."

| Date | Component | Performed by | Result | Notes |
|---|---|---|---|---|
| 2026-07-02 | DuckDB | Phase -1 build/test (automated, this session) | ✅ Pass | Backed up a 2-table/600-row test warehouse, restored to `/tmp/restore_drill.duckdb`, verified row counts matched (100 + 500 rows). Found and fixed a real bug: the `.sha256` sidecar stored a relative filename, so verification failed when the restore script was run from a different working directory than the backup — fixed by checking the checksum from within the backup's own directory. Also confirmed the safety refusal (no `--force` = no overwrite) works as intended. |
| _(none yet)_ | Postgres | — | — | Scheduled for Phase 0 completion, once Airflow stack is running with real DAG history to restore against. |
| _(none yet)_ | Kafka replay | — | — | Scheduled for Phase 3 completion. |

**Rule going forward: every phase that touches a backed-up component adds
a row here when its restore path is actually tested — not just written.**

| 2026-07-02 | DuckDB | You (local Windows machine) | ✅ Pass | Ran Backup-DuckDB.ps1 → banking_20260702T212029Z.duckdb, restored via Restore-DuckDB.ps1 to C:\temp\drill.duckdb. Checksum verified, restore opened cleanly. Confirmed 0 tables — banking.duckdb exists but is empty, meaning the pipeline hasn't been run end-to-end yet. Backup/restore mechanism itself proven working; Phase 0 will be what actually populates real data. |