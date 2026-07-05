{#
    macros/audit_columns.sql

    Returns a consistent set of audit columns to append to any mart model,
    so every row is traceable back to the exact dbt run that produced it —
    see docs/SECURITY_AND_GOVERNANCE.md and the audit trail design from
    Phase 1 planning.

    Usage in a model's final SELECT:
        SELECT
            ...,
            {{ audit_columns() }}
        FROM joined
#}
{% macro audit_columns() %}
    '{{ invocation_id }}'   AS _dbt_invocation_id,
    CURRENT_TIMESTAMP       AS _dbt_run_at
{% endmacro %}
