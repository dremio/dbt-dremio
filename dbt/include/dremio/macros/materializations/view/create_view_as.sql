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

{% macro dremio__create_view_as(relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
     {{exceptions.warn("Model contracts are not enforced by dbt-dremio!")}}
  {% endif %}

  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }} as {{ sql }}

{%- endmacro %}
