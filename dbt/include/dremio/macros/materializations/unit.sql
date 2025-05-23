{%- materialization unit, adapter='dremio' -%}

  {% set relations = [] %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set expected_sql = config.get('expected_sql') %}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length ) > 0 else get_columns_in_query(sql) %} %}

  {%- set relation_type = 'view' if this.database == target.database else 'table' -%}

  {%- set target_relation = this.incorporate(type=relation_type) -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}
  {%- set empty_sql = get_empty_subquery_sql(sql) -%}

  {% do run_query(
      get_create_view_as_sql(temp_relation, empty_sql)
      if temp_relation.type == 'view'
      else get_create_table_as_sql(True, temp_relation, empty_sql)
  ) %}

  {%- set columns_in_relation = adapter.get_columns_in_relation(temp_relation) -%}
  {%- set column_name_to_data_types = {} -%}
  {%- for column in columns_in_relation -%}
  {%-   do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
  {%- endfor -%}

  {% if not expected_sql %}
  {%   set expected_sql = get_expected_sql(expected_rows, column_name_to_data_types) %}
  {% endif %}
  {% set unit_test_sql = get_unit_test_sql(sql, expected_sql, tested_expected_column_names) %}

  {% call statement('main', fetch_result=True) -%}

    {{ unit_test_sql }}

  {%- endcall %}

  {% do adapter.drop_relation(temp_relation) %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
