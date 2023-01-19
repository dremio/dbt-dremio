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

{% macro dremio__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- set custom_alias_name = custom_alias_name if not is_datalake_node(node)
    else node.config.file -%}
  {{ generate_alias_name_impl(node.name, custom_alias_name, node) }}
{%- endmacro %}

{% macro generate_alias_name_impl(default_alias, custom_alias_name=none, node=none) -%}
  {%- if custom_alias_name is none -%}

      {{ default_alias }}

  {%- else -%}

      {{ custom_alias_name | trim }}

  {%- endif -%}
{%- endmacro %}
