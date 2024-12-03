-- {% macro persist_docs(relation, model, for_relation=true, for_columns=true) -%}
--   {{ return(adapter.dispatch('persist_docs', 'dbt')(relation, model, for_relation, for_columns)) }}
-- {% endmacro %}

-- {% macro default__persist_docs(relation, model, for_relation, for_columns) -%}
--   {% if for_relation and config.persist_relation_docs() and model.description %}
--     {% do run_query(alter_relation_comment(relation, model.description)) %}
--   {% endif %}

--   {% if for_columns and config.persist_column_docs() and model.columns %}
--     {% do run_query(alter_column_comment(relation, model.columns)) %}
--   {% endif %}
-- {% endmacro %}

{% macro dremio__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() %}
    {% do adapter.docs_integration_with_wikis(relation, model.description) %}
    {% do adapter.docs_integration_with_tags(relation, model.tags) %}
  {% endif %}
{% endmacro %}
