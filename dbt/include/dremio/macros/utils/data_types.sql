{% macro dremio__type_string() %}
    {{ return(api.Column.translate_type("VARCHAR")) }}
{% endmacro %}
