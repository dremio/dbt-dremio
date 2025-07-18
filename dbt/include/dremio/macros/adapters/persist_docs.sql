{% macro dremio__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() %}
    {# Build comprehensive wiki content #}
    {% set wiki_content = [] %}
    
    {# Add model name and description #}
    {% do wiki_content.append("# " ~ model.name) %}
    {% if model.description %}
      {% do wiki_content.append("\n## Description") %}
      {% do wiki_content.append(model.description) %}
    {% endif %}
    
    {# Add column information if available #}
    {% if model.columns %}
      {% do wiki_content.append("\n## Columns") %}
      {% do wiki_content.append("\n| Column | Data Type | Description |") %}
      {% do wiki_content.append("| --- | --- | --- |") %}
      
      {% for column_name, column_info in model.columns.items() %}
        {% set data_type = column_info.data_type if column_info.data_type else "N/A" %}
        {% set description = column_info.description if column_info.description else "No description" %}
        {% do wiki_content.append("| " ~ column_name ~ " | " ~ data_type ~ " | " ~ description ~ " |") %}
      {% endfor %}
    {% endif %}
    
    {# Add tests information if available #}
    {% if model.columns %}
      {% set has_tests = false %}
      {% for column_name, column_info in model.columns.items() %}
        {% if column_info.tests %}
          {% set has_tests = true %}
          {% break %}
        {% endif %}
      {% endfor %}
      
      {% if has_tests %}
        {% do wiki_content.append("\n## Data Tests") %}
        {% for column_name, column_info in model.columns.items() %}
          {% if column_info.tests %}
            {% do wiki_content.append("\n### " ~ column_name) %}
            {% for test in column_info.tests %}
              {% do wiki_content.append("- " ~ test) %}
            {% endfor %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}
    
    {# Add model tags if available #}
    {% if model.tags %}
      {% do wiki_content.append("\n## Tags") %}
      {% for tag in model.tags %}
        {% do wiki_content.append("- " ~ tag) %}
      {% endfor %}
    {% endif %}
    
    {# Join all content into a single string #}
    {% set full_wiki_content = wiki_content | join("\n") %}
    
    {# Pass the comprehensive wiki content to Dremio #}
    {% do adapter.process_wikis(relation, full_wiki_content) %}
    {% do adapter.process_tags(relation, model.tags) %}
  {% endif %}
{% endmacro %}
