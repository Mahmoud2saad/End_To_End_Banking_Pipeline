-- macros/generate_surrogate_key.sql
-- Project-level wrapper around dbt_utils.generate_surrogate_key.
-- Ensures consistent MD5-based key generation across all models,
-- and makes it easy to swap the hashing strategy in one place.
--
-- Usage (identical to dbt_utils version):
--   {{ generate_surrogate_key(['client_id']) }}
--   {{ generate_surrogate_key(['card_id', 'transaction_date']) }}

{% macro generate_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
