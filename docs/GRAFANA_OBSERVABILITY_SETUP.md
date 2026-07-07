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
