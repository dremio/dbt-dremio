{% macro dremio__safe_cast(field, type) %}
    {% if type.lower().startswith('interval') or (field|string).lower().startswith("'convert_from") %}
        {{field}}
    {% elif type == 'array' %}
        {{type ~ field}}
    {% else %}
        cast({{field}} as {{type}})
    {% endif %}
{% endmacro %}
