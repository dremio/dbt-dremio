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

{%- materialization clone, adapter= 'dremio' -%}

  {{ exceptions.raise_not_implemented('Clone command not supported in dbt-dremio. You may clone tables as views using twin_strategy.') }}
  {%- set relations = {'relations': []} -%}

  {%- if not defer_relation -%}
      -- nothing to do
      {{ log("No relation found in state manifest for " ~ model.unique_id, info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- if existing_relation and not flags.FULL_REFRESH -%}
      -- noop!
      {{ log("Relation " ~ existing_relation ~ " already exists", info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  -- Dremio does not support zero copy cloning of tables, so this will be cloned as a view
  {%- set target_relation = api.Relation.create(
          identifier=generate_alias_name_impl(model.name, config.get('file', validator=validation.any[basestring]), model),
          schema=generate_schema_name_impl(target.root_path, config.get('root_path', validator=validation.any[basestring]), model),
          database=generate_database_name_impl(target.datalake, config.get('datalake', validator=validation.any[basestring]), model),
          type='table') -%}

  {{exceptions.warn("Cloning tables is not supported. Applying twin strategy instead.")}}
  {%- set view_relation = api.Relation.create(
          identifier=generate_alias_name_impl(model.name, config.get('alias', validator=validation.any[basestring]), model),
          schema=generate_schema_name_impl(target.schema, config.get('schema', validator=validation.any[basestring]), model),
          database=generate_database_name_impl(target.database, config.get('database', validator=validation.any[basestring]), model),
          type='view') -%}

  {%- set sql_view -%}
          select *
          from {{ render_with_format_clause(target_relation) }}
  {%- endset -%}

  {% call statement('clone_view') -%}
    {{ create_view_as(view_relation, sql_view) }}
  {%- endcall %}


{%- endmaterialization -%}
