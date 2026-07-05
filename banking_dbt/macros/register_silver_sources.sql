{% macro register_silver_sources() %}

  {% set silver_base = env_var(
      'SILVER_PATH',
      '../local_warehouse/delta/silver'
  ) %}

  {% set silver_schema = target.schema %}
  {% set silver_database = target.database %}

  {% if execute %}

    {% call statement('install_delta_ext') %}
      INSTALL delta; LOAD delta;
    {% endcall %}

    {% call statement('create_silver_schema') %}
      CREATE SCHEMA IF NOT EXISTS {{ silver_database }}.{{ silver_schema }};
    {% endcall %}

    {% set tables = [
      'atm_master', 'customers', 'cards', 'card_transactions',
      'wallet_transactions', 'out_of_cash', 'kaggle_transactions',
      'pan_customer_map',
    ] %}

    {% for table in tables %}
      {% set path = silver_base ~ '/' ~ table %}
      {% call statement('register_' ~ table) %}
        CREATE OR REPLACE VIEW {{ silver_database }}.{{ silver_schema }}.{{ table }} AS
        SELECT * FROM delta_scan('{{ path }}');
      {% endcall %}
      {{ log("✓ Registered silver view: " ~ silver_database ~ "." ~ silver_schema ~ "." ~ table, info=True) }}
    {% endfor %}

    {% call statement('commit_silver_registration') %}
      COMMIT;
    {% endcall %}
    {{ log("✓ Committed silver schema to disk", info=True) }}

  {% endif %}

{% endmacro %}