-- macros/classify_risk_level.sql
-- Reusable risk level classifier.
--
-- Usage:
--   {{ classify_risk_level('fraud_rate_pct', 'debt_risk_level', 'dark_web_cards') }}

{% macro classify_risk_level(fraud_rate_col, debt_risk_col, dark_web_col) %}
    CASE
        WHEN {{ dark_web_col }} > 0             THEN 'CRITICAL'
        WHEN {{ fraud_rate_col }} > 10          THEN 'HIGH'
        WHEN {{ fraud_rate_col }} > 5
          OR {{ debt_risk_col }} = 'HIGH_RISK'  THEN 'MEDIUM'
        ELSE 'LOW'
    END
{% endmacro %}


-- Simpler single-score version used in gold models
-- {{ classify_risk_score(score_col) }}
{% macro classify_risk_score(score_col) %}
    CASE
        WHEN {{ score_col }} >= 75 THEN 'CRITICAL'
        WHEN {{ score_col }} >= 50 THEN 'HIGH'
        WHEN {{ score_col }} >= 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END
{% endmacro %}
