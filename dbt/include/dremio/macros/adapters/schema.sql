{% macro dremio__create_schema(relation) -%}
  {{ adapter.create_schema(relation) }}
{% endmacro %}

{% macro dremio__drop_schema(relation) -%}
{{ exceptions.raise_not_implemented(
  'drop_schema macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
