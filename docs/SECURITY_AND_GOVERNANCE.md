# Security & Data Governance

This document defines how the Banking Pipeline handles sensitive data,
secrets, and access control. It is the baseline every later phase (dbt,
Kafka, CI/CD) builds on top of — nothing here is retrofitted after the fact.

**Status of the data in this repo**: the shipped `data/synthetic/*.csv` files
contain synthetic placeholder values (`PAN = "Pan - 1"`, `card_number =
"Card - 4524"`) — not real card numbers. This is safe to develop against
as-is. Everything below describes the pattern to follow so the pipeline
would be safe to point at *real* PAN/card data without any redesign —
that's the actual point of doing this properly now.

---

## 1. PAN / Card Data Handling

**Rule: a real, unmasked PAN never crosses the producer boundary.**

```
Source system → [TOKENIZE HERE] → Kafka → Bronze → Silver → Gold
```

- Tokenization happens in the **producer**, before a single byte reaches
  Kafka. See `scripts/pan_tokenize.py` for the reference implementation:
  HMAC-SHA256 keyed by `PAN_TOKENIZATION_KEY`, truncated and formatted to
  preserve the last 4 digits for operational lookups (`****-****-****-4821`)
  while making the full number irreversible without the key.
- The tokenization key lives **only** in the secrets backend (Key
  Vault/Vault), never in `.env` past local dev, and is rotated on a defined
  schedule (recommend quarterly) with dual-write support during rotation.
- `dim_pan` (the fixed/renamed `pan_customer_map`) stores the token, never
  the raw PAN. If a real, reversible PAN lookup is ever needed
  operationally (e.g. fraud investigation), that's a *separate*, tightly
  RBAC'd service — not something dbt or Grafana ever has direct access to.
- This is why Phase 1's fix to actually wire `pan_key` into the fact grain
  matters for more than modeling correctness — it's the mechanism that lets
  fraud/dark-web analysis run entirely on tokens, never on raw PANs.

## 2. Secrets Management

**Rule: no secret in a file that gets committed, ever — including `.env`
past local dev.**

| Environment | Secrets source |
|---|---|
| `dev` (local laptop) | `.env` file (gitignored), fine for individual dev convenience |
| `staging` | Azure Key Vault (or HashiCorp Vault), pulled at container startup |
| `prod` | Azure Key Vault (or HashiCorp Vault), pulled at container startup, access logged and alerted on |

Migration path (`config.py` reads `SECRETS_BACKEND` and branches):
```python
if SECRETS_BACKEND == "env":
    value = os.getenv(key)
elif SECRETS_BACKEND == "azure_keyvault":
    value = keyvault_client.get_secret(key).value
elif SECRETS_BACKEND == "hashicorp_vault":
    value = vault_client.secrets.kv.read_secret_version(path=key)["data"]["data"]["value"]
```
This is implemented as `get_secret(key)` in the Phase 0 `config.py` patch —
every call site in the codebase goes through this function, never raw
`os.getenv()` for anything marked `[SECRET]` in `.env.example`.

Airflow-specific: connections/variables are Fernet-encrypted at rest
(`AIRFLOW__CORE__FERNET_KEY`), and in staging/prod Airflow should be
configured with a **Secrets Backend** (`AIRFLOW__SECRETS__BACKEND =
AzureKeyVaultBackend`) so connection passwords never sit in the metadata DB
at all.

## 3. RBAC

**Current state**: single shared `airflow/airflow` login, no Grafana auth
config, DuckDB has no user model at all. This is a demo pattern — fine for
`dev`, not acceptable past it.

| Role | Airflow | Grafana | dbt / warehouse |
|---|---|---|---|
| **Data Engineer** | Admin (dev/staging only) | Editor | Read/write `dev`/`staging` schemas |
| **Analyst** | Viewer | Viewer | Read-only `gold` schema, no `staging`/`snapshots` access |
| **On-call / Ops** | Op (can retry/clear tasks, not edit DAGs) | Viewer + alert ack | Read-only `gold.pipeline_health` |
| **Fraud/Compliance** | No access | Dedicated fraud dashboard only | Read-only `gold.fraud_risk_scoring`, never `dim_pan` token-to-identity mapping |
| **Service accounts** (Kafka consumer, n8n webhook receiver) | N/A | N/A | Scoped to exactly the tables they write/read, nothing else |

Concretely: Airflow's built-in RBAC (`AIRFLOW__WEBSERVER__RBAC=True`, already
the default in 2.x) gets real roles instead of one shared login; Grafana
gets an OAuth/LDAP backend instead of local-only auth in staging/prod;
DuckDB in `staging`/`prod` is replaced by a warehouse with real grants
(this is one of several reasons the README's cloud path to
Databricks/Snowflake matters for a "real" deployment — DuckDB has no
concept of row/column-level security).

## 4. Environment Separation

`ENV` now takes three values, not two: `dev | staging | prod` (previously
just `local | cloud`, which conflated "where does compute run" with "which
data am I allowed to touch" — two different questions).

| | dev | staging | prod |
|---|---|---|---|
| Storage root | `local_warehouse/` on laptop | isolated ADLS/S3 container | isolated ADLS/S3 container, separate subscription/account if possible |
| dbt schema | `banking_dev` | `banking_staging` | `banking_prod` |
| Kafka topic prefix | `banking.dev.*` | `banking.staging.*` | `banking.prod.*` |
| Data | Synthetic only | Masked/tokenized copy of prod-shaped data | Real (tokenized PAN as above) |
| Who can write | Any engineer | CI/CD pipeline only, after review | CI/CD pipeline only, after approval gate |

No path exists where a `dbt run` from a laptop can touch `prod` — the
`prod` target in `profiles.yml` requires credentials that only exist in the
CI/CD runner's secrets backend, not on any individual machine.

## 5. Data Retention & Deletion

- Bronze: retained per `KAFKA_RETENTION_HOURS`-equivalent policy on the
  Delta table (recommend 90 days rolling, enforced via scheduled `VACUUM` —
  see DR_RUNBOOK.md).
- Silver/Gold: retained per business requirement; SCD2 snapshots
  (`customers_snapshot`, `cards_snapshot`) already give you point-in-time
  history — define how long *that* history is kept, since it grows
  unbounded otherwise.
- Right-to-erasure pattern: because PAN is tokenized and customer PII sits
  in `dim_customer`, an erasure request is a targeted delete/anonymize on
  that dimension plus a documented downstream fact-table handling policy
  (recommend: retain the fact rows for financial record-keeping
  requirements, but null out the FK to the erased customer — this is a
  real design decision to make explicit, not solve silently).

---

**Next**: this document is referenced by Phase 0 (`config.py`'s
`get_secret()`), Phase 1 (`dim_pan` tokenization), Phase 3 (Kafka
`SASL_SSL` config), and Phase 5 (this file gets linked from the README,
not left as an internal doc nobody finds).