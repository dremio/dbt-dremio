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
  {%- set twin_strategy = config.get('twin_strategy', validator=validation.any[basestring]) or 'clone' -%}
  {%- if target_relation.type == 'view' -%}
    {%- if twin_strategy != 'allow' -%}
      {%- set table_relation = api.Relation.create(
          identifier=generate_alias_name_impl(model.name, config.get('file', validator=validation.any[basestring]), model),
          schema=generate_schema_name_impl(target.root_path, config.get('root_path', validator=validation.any[basestring]), model),
          database=generate_database_name_impl(target.datalake, config.get('datalake', validator=validation.any[basestring]), model),
          type='table') -%}
      {{ adapter.drop_relation(table_relation) }}
    {%- endif -%}
  {%- elif target_relation.type == 'table' -%}
    {%- if twin_strategy in ['prevent', 'clone'] -%}
      {%- set view_relation = api.Relation.create(
          identifier=generate_alias_name_impl(model.name, config.get('alias', validator=validation.any[basestring]), model),
          schema=generate_schema_name_impl(target.schema, config.get('schema', validator=validation.any[basestring]), model),
          database=generate_database_name_impl(target.database, config.get('database', validator=validation.any[basestring]), model),
          type='view') -%}
      {%- if twin_strategy == 'prevent' -%}
        {{ adapter.drop_relation(view_relation) }}
      {%- elif twin_strategy == 'clone' -%}
        {%- set sql_view -%}
          select *
          from {{ render_with_format_clause(target_relation) }}
        {%- endset -%}
        {% call statement('clone_view') -%}
          {{ create_view_as(view_relation, sql_view) }}
        {%- endcall %}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}
