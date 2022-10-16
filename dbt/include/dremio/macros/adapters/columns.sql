{% macro dremio__get_columns_in_relation(relation) -%}

  {%- set database = relation.database.strip('"') -%}
  {%- set schema = relation.schema.strip('"') -%}
  {%- set identifier = relation.identifier.strip('"') -%}
  {%- set schema_name = database
        + (('.' + schema) if schema != 'no_schema' else '') -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select column_name as column_name
        ,lower(data_type) as data_type
        ,character_maximum_length
        ,numeric_precision
        ,numeric_scale
    from information_schema.columns
    where ilike(table_schema, '{{ schema_name }}')
    and ilike(table_name, '{{ identifier }}')
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}

{% macro dremio__alter_column_type(relation, column_name, new_column_type) -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter column {{ adapter.quote(column_name) }} {{ adapter.quote(column_name) }} {{ new_column_type }}
  {% endcall %}

{% endmacro %}

{% macro dremio__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}

  {% if add_columns | length > 0 %}
    {% set sql -%}
       alter {{ relation.type }} {{ relation }} add columns (

              {% for column in add_columns %}
                 {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
              {% endfor %}
        )
    {%- endset -%}
    {% do run_query(sql) %}
  {% endif %}

  {% if remove_columns | length > 0 %}
    {% for column in remove_columns %}
      {% set sql -%}
         alter {{ relation.type }} {{ relation }} drop column {{ column.name }}
      {%- endset -%}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro intersect_columns(source_columns, target_columns) %}

  {% set result = [] %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}

   {# --check whether the name attribute exists in the target - this does not perform a data type check #}
   {% for sc in source_columns %}
     {% if sc.name in target_names %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}

  {{ return(result) }}

{% endmacro %}

{% macro get_quoted_csv(column_names, table_alias=none) %}

    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append((adapter.quote(table_alias) ~ '.' if table_alias is not none else '') ~ adapter.quote(col)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}

{% macro diff_columns(source_columns, target_columns) %}

  {% set result = [] %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}

   {# --check whether the name attribute exists in the target - this does not perform a data type check #}
   {% for sc in source_columns %}
     {% if sc.name not in target_names %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}

  {{ return(result) }}

{% endmacro %}

