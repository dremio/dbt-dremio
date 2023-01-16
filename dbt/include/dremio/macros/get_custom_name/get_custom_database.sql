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

{% macro dremio__generate_database_name(custom_database_name=none, node=none) -%}
  {%- set default_database = target.database if not is_datalake_node(node)
    else target.datalake -%}
  {%- set custom_database_name = custom_database_name if not is_datalake_node(node)
    else node.config.datalake -%}
  {{ generate_database_name_impl(default_database, custom_database_name, node) }}
{%- endmacro %}

{% macro generate_database_name_impl(default_database, custom_database_name=none, node=none) -%}
  {%- if custom_database_name is none -%}

      {{ default_database }}

  {%- else -%}

      {{ custom_database_name }}

  {%- endif -%}
{%- endmacro %}
