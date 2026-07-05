-- macros/convert_to_mad.sql
-- Converts an amount from a given currency to MAD (Moroccan Dirham).
-- Exchange rates are approximate and can be overridden via dbt vars.
--
-- Usage:
--   {{ convert_to_mad('amount_usd', 'USD') }}
--   {{ convert_to_mad('amount_eur', 'EUR') }}

{% macro convert_to_mad(amount_col, currency_col_or_literal, is_literal=true) %}

    {% set rates = {
        'MAD': 1.0,
        'USD': 10.0,
        'EUR': 10.9,
        'GBP': 12.7,
        'AED': 2.72,
        'SAR': 2.67,
    } %}

    {% if is_literal %}
        {# currency passed as a string literal e.g. 'USD' #}
        {% set rate = rates.get(currency_col_or_literal, 1.0) %}
        ROUND(CAST({{ amount_col }} AS DOUBLE) * {{ rate }}, 2)
    {% else %}
        {# currency passed as a column name — use CASE #}
        ROUND(
            CAST({{ amount_col }} AS DOUBLE) *
            CASE {{ currency_col_or_literal }}
                {% for currency, rate in rates.items() %}
                WHEN '{{ currency }}' THEN {{ rate }}
                {% endfor %}
                ELSE 1.0
            END
        , 2)
    {% endif %}

{% endmacro %}
