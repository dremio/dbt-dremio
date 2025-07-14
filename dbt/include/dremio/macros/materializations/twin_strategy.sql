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

{%- macro apply_twin_strategy(target_relation) -%}
  {%- set object_storage_source = target.datalake -%}
  {%- set object_storage_path = target.root_path -%}
  {%- set dremio_space = target.database -%}
  {%- set dremio_space_folder = target.schema -%}

  {%- set using_enterprise_catalog = (object_storage_source == dremio_space and object_storage_path == dremio_space_folder) -%}

  {%- if using_enterprise_catalog -%}
    {%- if config.get('twin_strategy') is not none -%}
      {% do exceptions.warn("WARNING: Twin strategy not applied - using enterprise catalog") %}
    {%- endif -%}
  {%- else -%}
    {%- set twin_strategy = config.get('twin_strategy', validator=validation.any[basestring]) or 'clone' -%}
    
    {%- set view_relation = api.Relation.create(
      identifier=generate_alias_name_impl(model.name, config.get('alias', validator=validation.any[basestring]), model),
      schema=generate_schema_name_impl(target.schema, config.get('schema', validator=validation.any[basestring]), model),
      database=generate_database_name_impl(target.database, config.get('database', validator=validation.any[basestring]), model),
      type='view') -%}

    {%- set conflicting_relation = none -%}
    {%- if target_relation.type == 'view' -%}
      {%- set table_relation = api.Relation.create(
          identifier=generate_alias_name_impl(model.name, config.get('file', validator=validation.any[basestring]), model),
          schema=generate_schema_name_impl(target.root_path, config.get('root_path', validator=validation.any[basestring]), model),
          database=generate_database_name_impl(target.datalake, config.get('datalake', validator=validation.any[basestring]), model),
          type='table') -%}
      {%- set conflicting_relation = adapter.get_relation(database=table_relation.database, schema=table_relation.schema, identifier=table_relation.identifier) -%}
    {%- elif target_relation.type == 'table' -%}
      {%- set conflicting_relation = adapter.get_relation(database=view_relation.database, schema=view_relation.schema, identifier=view_relation.identifier) -%}
    {%- endif -%}

    {%- if conflicting_relation is not none -%}
      {%- if twin_strategy == 'prevent' -%}
        -- Prevent strategy -> Drop the conflicting relation
        {{ adapter.drop_relation(conflicting_relation) }}
      {%- elif twin_strategy == 'clone' -%}
        -- Clone strategy -> Create a view selecting from the correct relation
        {%- set sql_view -%}
          select *
          from {{ render_with_format_clause(
            target_relation.type == 'view' and conflicting_relation or target_relation
          ) }}
        {%- endset -%}
        {% call statement('clone_relation') -%}
          {{ create_view_as(view_relation, sql_view) }}
        {%- endcall %} 
      {%- endif -%}
    {%- endif -%}    
  {%- endif -%}
{%- endmacro -%}
