{% macro current_timestamp() -%}
  (SELECT CURRENT_TIMESTAMP())
{%- endmacro %}