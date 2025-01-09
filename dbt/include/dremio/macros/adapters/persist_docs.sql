{% macro dremio__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() %}
    {% do adapter.process_wikis(relation, model.description) %}
    {% do adapter.process_tags(relation, model.tags) %}
  {% endif %}
{% endmacro %}
