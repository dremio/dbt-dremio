{% macro default__validate_sql(sql) -%}
  {% call statement('validate_sql') -%}
    explain plan for {{ sql }}
  {% endcall %}
  {{ return(load_result('validate_sql')) }}
{% endmacro %}

