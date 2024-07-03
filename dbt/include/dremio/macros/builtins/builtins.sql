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

{%- macro ref(model_name, v=None) -%}
  {%- set relation = builtins.ref(model_name, v=v) -%}
  {%- if execute -%}
    {%- set model = graph.nodes.values() | selectattr("name", "equalto", model_name) | list | first -%}
    {%- if model.config.materialized == 'reflection' -%}
      {% do exceptions.CompilationError("Reflections cannot be ref()erenced (" ~ relation ~ ")") %}
    {%- endif -%}
    {%- set format = model.config.format if
      model.config.materialized not in ['view', 'reflection']
      and model.config.format is defined
      else none -%}
    {%- set format_clause = format_clause_from_node(model.config) if format is not none else none -%}
    {%- set relation2 = api.Relation.create(database=relation.database, schema=relation.schema, identifier=relation.identifier, format=format, format_clause=format_clause) -%}
    {{ return (relation2) }}
  {%- else -%}
    {{ return (relation) }}
  {%- endif -%}
{%- endmacro -%}

{%- macro source(source_name, table_name) -%}
  {%- set relation = builtins.source(source_name, table_name) -%}
  {%- if execute -%}
    {%- set source = graph.sources.values() | selectattr("source_name", "equalto", source_name) | selectattr("name", "equalto", table_name) | list | first -%}
    {%- set format = source.external.format if
      source.external is defined
      and source.external.format is defined
      else none -%}
    {%- set format_clause = format_clause_from_node(source.external) if format is not none else none -%}
    {%- set relation2 = api.Relation.create(database=relation.database, schema=relation.schema, identifier=relation.identifier, format=format, format_clause=format_clause) -%}
    {{ return (relation2) }}
  {%- else -%}
    {{ return (relation) }}
  {%- endif -%}
{%- endmacro -%}
