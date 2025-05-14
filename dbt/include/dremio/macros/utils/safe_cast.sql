{% macro dremio__safe_cast(field, type) %}
    {% if type != 'array' %}
        cast({{field}} as {{type}})
    {% else %}
        {{type ~ field}}
    {% endif %}
{% endmacro %}
