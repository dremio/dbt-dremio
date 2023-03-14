/*Copyright (C) 2022 Dremio Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

{% macro dremio__get_catalog(information_schema, schemas) -%}
  {%- set database = information_schema.database.strip('"') -%}
  {%- set table_schemas = [] -%}
  {%- for schema in schemas -%}
    {%- set schema = schema.strip('"') -%}
    {%- do table_schemas.append(
      "'" + database + (('.' + schema) if schema != 'no_schema' else '') + "'"
    ) -%}
  {%- endfor -%}
  {%- call statement('catalog', fetch_result=True) -%}

    with cte as (

    {%- if var('dremio:reflections_enabled', default=false) %}

      select
        case when position('.' in table_schema) > 0
                then substring(table_schema, 1, position('.' in table_schema) - 1)
                else table_schema
            end as table_database
        ,case when position('.' in table_schema) > 0
                then substring(table_schema, position('.' in table_schema) + 1)
                else 'no_schema'
            end as table_schema
        ,reflection_name as table_name
        ,'materializedview' as table_type
        ,case
            when nullif(external_reflection, '') is not null then 'target: ' || external_reflection
            when arrow_cache then 'arrow cache'
        end as table_comment
        ,column_name
        ,ordinal_position as column_index
        ,lower(data_type) as column_type
        ,concat(
        case when strpos(regexp_replace(display_columns, '$|, |^', '/'), '/' || column_name || '/') > 0 then 'display' end
        ,', ',case when strpos(regexp_replace(dimensions, '$|, |^', '/'), '/' || column_name || '/') > 0 then 'dimension'  end
        ,', ',case when strpos(regexp_replace(measures, '$|, |^', '/'), '/' || column_name || '/') > 0 then 'measure'  end
        ,', ',case when strpos(regexp_replace(sort_columns, '$|, |^', '/'), '/' || column_name || '/') > 0 then 'sort'  end
        ,', ',case when strpos(regexp_replace(partition_columns, '$|, |^', '/'), '/' || column_name || '/') > 0 then 'partition'  end
        ,', ',case when strpos(regexp_replace(distribution_columns, '$|, |^', '/'), '/' || column_name || '/') > 0 then 'distribution' end
        ) as column_comment
        ,cast(null as varchar) as table_owner
      from sys.reflections
      join information_schema.columns
          on (columns.table_schema || '.' || columns.table_name = replace(dataset_name, '"', '')
            and (strpos(regexp_replace(display_columns, '$|, |^', '/'), '/' || column_name || '/') > 0
                  or strpos(regexp_replace(dimensions, '$|, |^', '/'), '/' || column_name || '/') > 0
                  or strpos(regexp_replace(measures, '$|, |^', '/'), '/' || column_name || '/') > 0 ))
      where
        {% for table_schema in table_schemas -%}
          ilike( table_schema, {{ table_schema.strip('"') }}){%- if not loop.last %} or {% endif -%}
        {%- endfor %}

      union all

    {%- endif %}

      select (case when position('.' in columns.table_schema) > 0
            then substring(columns.table_schema, 1, position('.' in columns.table_schema) - 1)
            else columns.table_schema
        end) as table_database
        ,(case when position('.' in columns.table_schema) > 0
            then substring(columns.table_schema, position('.' in columns.table_schema) + 1)
            else 'no_schema'
        end) as table_schema
        ,columns.table_name
        ,lower(t.table_type) as table_type
        ,cast(null as varchar) as table_comment
        ,column_name
        ,ordinal_position as column_index
        ,lower(data_type) as column_type
        ,cast(null as varchar) as column_comment
        ,cast(null as varchar) as table_owner
      from information_schema."tables" as t
      join information_schema.columns
        on (t.table_schema = columns.table_schema
        and t.table_name = columns.table_name)
      where t.table_type <> 'SYSTEM_TABLE'
      and (
        {%- for table_schema in table_schemas -%}
          ilike( t.table_schema, {{ table_schema.strip('"') }}){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
    )

    select *
    from cte
    order by table_schema
      ,table_name
      ,column_index
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}

{% macro dremio__information_schema_name(database) -%}
    information_schema
{%- endmacro %}

{% macro dremio__list_schemas(database) -%}
  {%- set schema_name_like = database.strip('"') + '.%' -%}
  {% set sql %}
    select substring(schema_name, position('.' in schema_name) + 1)
    from information_schema.schemata
    where ilike(schema_name, '{{ schema_name_like }}')
    union
    values('no_schema')
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro dremio__check_schema_exists(information_schema, schema) -%}
  {%- set schema_name = information_schema.database.strip('"')
        + (('.' + schema) if schema != 'no_schema' else '') -%}
  {% set sql -%}
    select count(*)
    from information_schema.schemata
    where ilike(schema_name, '{{ schema_name }}')
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro dremio__list_relations_without_caching(schema_relation) %}
  {%- set database = schema_relation.database.strip('"') -%}
  {%- set schema = schema_relation.schema.strip('"') -%}
  {%- set schema_name = database
        + (('.' + schema) if schema != 'no_schema' else '') -%}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}

    {%- if var('dremio:reflections_enabled', default=false) -%}

      with cte1 as (
        select
          dataset_name
          ,reflection_name
          ,type
          ,case when substr(dataset_name, 1, 1) = '"'
          then strpos(dataset_name, '".') + 1
          else strpos(dataset_name, '.')
          end as first_dot
          ,length(dataset_name) -
          case when substr(dataset_name, length(dataset_name)) = '"'
          then strpos(reverse(dataset_name), '".')
          else strpos(reverse(dataset_name), '.') - 1
          end as last_dot
          ,length(dataset_name) as length
        from sys.reflections
      )
      , cte2 as (
        select
          replace(substr(dataset_name, 1, first_dot - 1), '"', '') as table_catalog
          ,reflection_name as table_name
          ,replace(case when first_dot < last_dot
          then substr(dataset_name, first_dot + 1, last_dot - first_dot - 1)
          else 'no_schema' end, '"', '') as table_schema
          ,'materializedview' as table_type
        from cte1
      )
      select table_catalog, table_name, table_schema, table_type
      from cte2
      where ilike(table_catalog, '{{ database }}')
      and ilike(table_schema, '{{ schema }}')

      union all

    {%- endif %}

      select (case when position('.' in table_schema) > 0
              then substring(table_schema, 1, position('.' in table_schema) - 1)
              else table_schema
          end) as table_catalog
          ,table_name
          ,(case when position('.' in table_schema) > 0
              then substring(table_schema, position('.' in table_schema) + 1)
              else 'no_schema'
          end) as table_schema
          ,lower(table_type) as table_type
      from information_schema."tables"
      {%- if var('dremio:exact_search_enabled', default=false) %}
        where table_schema = '{{ schema_name }}'
      {%- else %}
        where ilike(table_schema, '{{ schema_name }}')
      {%- endif %}
      and table_type <> 'system_table'

  {% endcall %}
  {% set t = load_result('list_relations_without_caching').table %}
  {{ return(t) }}
{% endmacro %}
