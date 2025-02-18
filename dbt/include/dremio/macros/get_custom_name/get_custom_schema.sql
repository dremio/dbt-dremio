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

{% macro dremio__generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema if not is_datalake_node(node)
    else target.root_path -%}
  {%- set custom_schema_name = custom_schema_name if not is_datalake_node(node)
    else node.config.root_path -%}
  {%- set custom_schema_name = append_schema(custom_schema_name or default_schema, node.config.schema)
    if node.config.materialized == 'table' else custom_schema_name -%}
  {{ generate_schema_name_impl(default_schema, custom_schema_name, node) }}
{%- endmacro %}

{% macro append_schema(base_path, subfolder) -%}
  {{ base_path if subfolder is none else subfolder if base_path in [none, 'no_schema'] else base_path ~ '.' ~ subfolder }}
{%- endmacro %}

{% macro generate_schema_name_impl(default_schema, custom_schema_name=none, node=none) -%}
  {%- if custom_schema_name is none -%}

      {{ default_schema }}

  {%- else -%}

      {{ custom_schema_name }}

  {%- endif -%}
{%- endmacro %}
