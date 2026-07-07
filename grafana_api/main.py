"""
Lightweight FastAPI bridge between Grafana and DuckDB.
Grafana JSON datasource calls this API to query gold layer models.

Usage--from the project folder root:
uvicorn grafana_api.main:app --host 0.0.0.0 --port 8000 --reload


Available endpoints:
    GET /                               Health check — confirms API and DuckDB connection are live.
    GET /dwh-info                       Available tables, their types and schemas.
    
    GET /atm-performance                Daily ATM metrics per terminal: transactions, success rate, OOC events, performance grade (A–D).
    GET /atm-performance/by-region      ATM metrics aggregated by governorate and performance grade — good for bar/map charts.
    GET /atm-performance/by-grade       ATM count and avg success rate grouped by grade A–D — good for pie charts.

    GET /replenishment                  Daily cash replenishment status per ATM: utilization %, urgency (CRITICAL/HIGH/MEDIUM/LOW), OOC events.
    GET /replenishment/by-urgency       Replenishment urgency distribution across all ATMs — good for summary stat panels.

    GET /fraud-risk                     Full fraud risk profile per customer: score (0–100), risk level, dark web cards, fraud rate.
    GET /fraud-risk/summary             Fraud metrics aggregated by risk level, income segment, and age group — good for charts.

    GET /channel-comparison             Hourly channel metrics (ATM / Wallet / Card): transactions, amount, success rate, time of day.
    GET /channel-comparison/daily       Same metrics aggregated to daily level per channel — good for time-series charts.
    GET /channel-comparison/by-time     Channel performance broken down by time of day (MORNING/AFTERNOON/EVENING/NIGHT).

    GET /customer-spending              Full spending profile per customer: activity level, preferred category, online ratio, spend-to-income.
    GET /customer-spending/summary      Customer counts and avg spend aggregated by activity level, income segment, and category.

    GET /governorate-summary            Regional roll-up by Moroccan governorate: ATM count, transaction volume, OOC rate, wallet share.

    GET /pipeline-health                Last dbt run results: model/test name, status (success/error), duration, and failure count.
"""

import json
import math
import os
import math
import numpy as np
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Banking Pipeline — Grafana API")

# Allow Grafana (localhost:3000) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Path to DuckDB warehouse 
DB_PATH = os.environ.get(
    "DUCKDB_PATH",
    r"D:\Banking_Pipeline\test\local_warehouse\banking.duckdb"
)



def run_query(sql: str) -> list[dict]:
    """Execute a SQL query against DuckDB and return JSON-safe rows."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        result = con.execute(sql).fetchdf()
        con.close()

        # Convert to records first, then sanitize every value individually
        records = result.to_dict(orient="records")

        def make_safe(value):
            # Handle numpy scalar types
            if isinstance(value, (np.integer,)):
                return int(value)
            if isinstance(value, (np.floating,)):
                if np.isnan(value) or np.isinf(value):
                    return None
                return float(value)
            if isinstance(value, np.bool_):
                return bool(value)
            # Handle Python native float NaN/Inf
            if isinstance(value, float):
                if math.isnan(value) or math.isinf(value):
                    return None
            # Handle pandas NaT and None
            if value is None:
                return None
            try:
                import pandas as pd
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                pass
            return value

        clean = [
            {k: make_safe(v) for k, v in row.items()}
            for row in records
        ]

        return clean

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "ok", "db": DB_PATH}


# ── Discover Data Warehouse──────────────────────────────────────────────────────────────
@app.get("/dwh-info")
def dwh_info():
    return run_query("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name;
    """)

# ── ATM Performance ───────────────────────────────────────────────────────────
# Columns: atm_key, atm_id, region, atm_type, provider, cash_limit_mad,
#          is_cash_deposit_enabled, capacity_tier, transaction_date,
#          card_transactions, successful_card_txns, reversals, deposits,
#          card_amount_mad, avg_card_amount_mad, ooc_events,
#          attempted_amount_mad, wallet_transactions, wallet_amount_mad,
#          success_rate_pct, total_transactions, total_amount_mad,
#          performance_grade, _loaded_at
@app.get("/atm-performance")
def atm_performance():
    return run_query("""
        SELECT
            atm_id,
            region,
            atm_type,
            provider,
            capacity_tier,
            is_cash_deposit_enabled,
            transaction_date,
            card_transactions,
            successful_card_txns,
            reversals,
            deposits,
            card_amount_mad,
            avg_card_amount_mad,
            ooc_events,
            attempted_amount_mad,
            wallet_transactions,
            wallet_amount_mad,
            success_rate_pct,
            total_transactions,
            total_amount_mad,
            performance_grade
        FROM banking_dev_marts.atm_performance
        ORDER BY transaction_date DESC, total_transactions DESC
    """)


# ── ATM Performance Summary (aggregated per region for charts) ────────────────
@app.get("/atm-performance/by-region")
def atm_performance_by_region():
    return run_query("""
        SELECT
            region,
            performance_grade,
            COUNT(DISTINCT atm_id)              AS atm_count,
            SUM(total_transactions)             AS total_transactions,
            SUM(total_amount_mad)               AS total_amount_mad,
            ROUND(AVG(success_rate_pct), 2)     AS avg_success_rate_pct,
            SUM(ooc_events)                     AS total_ooc_events,
            SUM(attempted_amount_mad)           AS total_lost_revenue_mad
        FROM banking_dev_marts.atm_performance
        GROUP BY region, performance_grade
        ORDER BY total_transactions DESC
    """)


# ── ATM Performance Summary (aggregated per grade for pie chart) ──────────────
@app.get("/atm-performance/by-grade")
def atm_performance_by_grade():
    return run_query("""
        SELECT
            performance_grade,
            COUNT(DISTINCT atm_id)              AS atm_count,
            ROUND(AVG(success_rate_pct), 2)     AS avg_success_rate_pct,
            SUM(ooc_events)                     AS total_ooc_events
        FROM banking_dev_marts.atm_performance
        GROUP BY performance_grade
        ORDER BY performance_grade
    """)


# ── Replenishment Analysis ────────────────────────────────────────────────────
# Columns: atm_key, atm_id, region, atm_type, provider, cash_limit_mad,
#          capacity_tier, activity_date, dispensed_mad, card_transactions,
#          ooc_events, attempted_after_empty_mad, first_ooc_hour,
#          cash_utilization_pct, ran_out_of_cash, replenishment_urgency,
#          replenishment_status, _loaded_at
@app.get("/replenishment")
def replenishment():
    return run_query("""
        SELECT
            atm_id,
            region,
            provider,
            capacity_tier,
            cash_limit_mad,
            activity_date,
            dispensed_mad,
            card_transactions,
            ooc_events,
            attempted_after_empty_mad,
            first_ooc_hour,
            cash_utilization_pct,
            ran_out_of_cash,
            replenishment_urgency,
            replenishment_status
        FROM banking_dev_marts.replenishment_analysis
        ORDER BY
            CASE replenishment_urgency
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH'     THEN 2
                WHEN 'MEDIUM'   THEN 3
                ELSE                 4
            END,
            cash_utilization_pct DESC
    """)


# ── Replenishment urgency distribution (for pie/bar chart) ───────────────────
@app.get("/replenishment/by-urgency")
def replenishment_by_urgency():
    return run_query("""
        SELECT
            replenishment_urgency,
            replenishment_status,
            COUNT(DISTINCT atm_id)              AS atm_count,
            SUM(ooc_events)                     AS total_ooc_events,
            ROUND(AVG(cash_utilization_pct), 2) AS avg_utilization_pct,
            SUM(attempted_after_empty_mad)      AS total_lost_mad
        FROM banking_dev_marts.replenishment_analysis
        GROUP BY replenishment_urgency, replenishment_status
        ORDER BY total_ooc_events DESC
    """)


# ── Fraud Risk Scoring ────────────────────────────────────────────────────────
# Columns: customer_key, client_id, income_segment, credit_tier,
#          debt_risk_level, age_group, gender, country,
#          total_transactions, fraud_transactions, total_amount_mad,
#          avg_amount_mad, max_amount_mad, fraud_rate_pct,
#          online_transactions, active_days, total_cards, dark_web_cards,
#          expired_cards, max_credit_limit_mad, fraud_risk_score,
#          risk_level, _loaded_at
@app.get("/fraud-risk")
def fraud_risk():
    return run_query("""
        SELECT
            client_id,
            income_segment,
            credit_tier,
            debt_risk_level,
            age_group,
            gender,
            country,
            total_transactions,
            fraud_transactions,
            total_amount_mad,
            avg_amount_mad,
            max_amount_mad,
            fraud_rate_pct,
            online_transactions,
            active_days,
            total_cards,
            dark_web_cards,
            expired_cards,
            max_credit_limit_mad,
            fraud_risk_score,
            risk_level
        FROM banking_dev_marts.fraud_risk_scoring
        ORDER BY fraud_risk_score DESC
    """)


# ── Fraud risk distribution (for dashboard charts) ───────────────────────────
@app.get("/fraud-risk/summary")
def fraud_risk_summary():
    return run_query("""
        SELECT
            risk_level,
            income_segment,
            age_group,
            gender,
            COUNT(*)                            AS customer_count,
            ROUND(AVG(fraud_risk_score), 2)     AS avg_risk_score,
            SUM(fraud_transactions)             AS total_fraud_txns,
            ROUND(AVG(fraud_rate_pct), 4)       AS avg_fraud_rate_pct,
            SUM(dark_web_cards)                 AS total_dark_web_cards
        FROM banking_dev_marts.fraud_risk_scoring
        GROUP BY risk_level, income_segment, age_group, gender
        ORDER BY avg_risk_score DESC
    """)


# ── Channel Comparison ────────────────────────────────────────────────────────
# Columns: channel_key, channel_code, transaction_date, transaction_hour,
#          transactions, total_amount_mad, avg_amount_mad, successful_txns,
#          reversals, deposits, success_rate_pct, time_of_day, _loaded_at
@app.get("/channel-comparison")
def channel_comparison():
    return run_query("""
        SELECT
            channel_code,
            transaction_date,
            transaction_hour,
            time_of_day,
            transactions,
            total_amount_mad,
            avg_amount_mad,
            successful_txns,
            reversals,
            deposits,
            success_rate_pct
        FROM banking_dev_marts.channel_comparison
        ORDER BY transaction_date DESC, channel_code, transaction_hour
    """)


# ── Channel daily aggregation (for time-series charts) ───────────────────────
@app.get("/channel-comparison/daily")
def channel_comparison_daily():
    return run_query("""
        SELECT
            channel_code,
            transaction_date,
            SUM(transactions)               AS total_transactions,
            SUM(total_amount_mad)           AS total_amount_mad,
            ROUND(AVG(avg_amount_mad), 2)   AS avg_amount_mad,
            SUM(successful_txns)            AS successful_txns,
            SUM(reversals)                  AS reversals,
            ROUND(
                SUM(successful_txns) * 100.0
                / NULLIF(SUM(transactions), 0), 2
            )                               AS success_rate_pct
        FROM banking_dev_marts.channel_comparison
        GROUP BY channel_code, transaction_date
        ORDER BY transaction_date DESC, channel_code
    """)


# ── Channel by time of day (for heatmap) ─────────────────────────────────────
@app.get("/channel-comparison/by-time")
def channel_by_time():
    return run_query("""
        SELECT
            channel_code,
            time_of_day,
            SUM(transactions)               AS total_transactions,
            ROUND(AVG(success_rate_pct), 2) AS avg_success_rate_pct
        FROM banking_dev_marts.channel_comparison
        GROUP BY channel_code, time_of_day
        ORDER BY channel_code, time_of_day
    """)


# ── Customer Spending Behavior ────────────────────────────────────────────────
# Columns: customer_key, client_id, income_segment, credit_tier, age_group,
#          gender, country, yearly_income_mad, credit_score,
#          total_transactions, total_spend_mad, avg_spend_mad, max_spend_mad,
#          morning_txns, afternoon_txns, evening_txns, night_txns,
#          online_txns, fraud_txns, active_days, unique_categories,
#          preferred_category, spend_to_income_ratio, online_ratio_pct,
#          activity_level, _loaded_at
@app.get("/customer-spending")
def customer_spending():
    return run_query("""
        SELECT
            client_id,
            income_segment,
            credit_tier,
            age_group,
            gender,
            country,
            yearly_income_mad,
            credit_score,
            total_transactions,
            total_spend_mad,
            avg_spend_mad,
            max_spend_mad,
            morning_txns,
            afternoon_txns,
            evening_txns,
            night_txns,
            online_txns,
            fraud_txns,
            active_days,
            unique_categories,
            preferred_category,
            spend_to_income_ratio,
            online_ratio_pct,
            activity_level
        FROM banking_dev_marts.customer_spending_behavior
        ORDER BY total_spend_mad DESC
    """)


# ── Customer segments summary (for dashboard charts) ─────────────────────────
@app.get("/customer-spending/summary")
def customer_spending_summary():
    return run_query("""
        SELECT
            activity_level,
            income_segment,
            age_group,
            preferred_category,
            COUNT(*)                            AS customer_count,
            ROUND(AVG(total_spend_mad), 2)      AS avg_total_spend_mad,
            ROUND(AVG(avg_spend_mad), 2)        AS avg_transaction_mad,
            ROUND(AVG(online_ratio_pct), 2)     AS avg_online_ratio_pct,
            ROUND(AVG(spend_to_income_ratio), 2) AS avg_spend_to_income,
            SUM(fraud_txns)                     AS total_fraud_txns
        FROM banking_dev_marts.customer_spending_behavior
        GROUP BY activity_level, income_segment, age_group, preferred_category
        ORDER BY customer_count DESC
    """)


# ── Governorate Summary ───────────────────────────────────────────────────────
# Columns: region, country, macro_region, population_tier, total_atms,
#          atm_transactions, atm_amount_mad, successful_atm_txns,
#          atm_reversals, wallet_transactions, wallet_amount_mad,
#          successful_wallet_txns, ooc_events, ooc_attempted_mad,
#          total_transactions, total_amount_mad, atm_success_rate_pct,
#          ooc_rate_pct, wallet_share_pct, _loaded_at
@app.get("/governorate-summary")
def governorate_summary():
    return run_query("""
        SELECT
            region,
            country,
            macro_region,
            population_tier,
            total_atms,
            atm_transactions,
            atm_amount_mad,
            successful_atm_txns,
            atm_reversals,
            wallet_transactions,
            wallet_amount_mad,
            successful_wallet_txns,
            ooc_events,
            ooc_attempted_mad,
            total_transactions,
            total_amount_mad,
            atm_success_rate_pct,
            ooc_rate_pct,
            wallet_share_pct
        FROM banking_dev_marts.governorate_summary
        ORDER BY total_transactions DESC
    """)


# ── Pipeline Health ───────────────────────────────────────────────────────────
@app.get("/pipeline-health")
def pipeline_health():
    """Reads dbt run_results.json to show last pipeline execution status."""
    results_path = os.environ.get(
        "DBT_RUN_RESULTS",
        r"D:\Banking_Pipeline\test\banking_dbt\target\run_results.json"
    )
    if not os.path.exists(results_path):
        return {"error": "run_results.json not found — run dbt first"}

    with open(results_path) as f:
        data = json.load(f)

    results = []
    for r in data.get("results", []):
        uid = r.get("unique_id", "")
        node_type = uid.split(".")[0] if "." in uid else "unknown"
        node_id = uid.split(".")[-1] if "." in uid else uid
        node_name = uid.split(".")[-2] if "." in uid else uid
        results.append({
            "node_id":      node_id,
            "node_name":    node_name,
            "type":      node_type,
            "status":    r.get("status"),
            "duration":  round(r.get("execution_time", 0), 2),
            "failures":  r.get("failures", 0),
            "message":   r.get("message", ""),
        })

    return results



@app.get("/customers")
def customers():
    return run_query("select * from banking_dev_marts.dim_customer")


# --- Phase 4: observability endpoints (kafka-lag, backup-health, freshness) ---
from grafana_api.observability import router as observability_router
app.include_router(observability_router)

