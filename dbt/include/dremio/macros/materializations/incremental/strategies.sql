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

{% macro get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set src_columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- set intersection = intersect_columns(src_columns, dest_columns) -%}
    {%- set dest_cols_csv = intersection | map(attribute='quoted') | join(', ') -%}
    insert into {{ target_relation }}( {{dest_cols_csv}} )
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

{% macro dbt_dremio_get_incremental_sql(strategy, source, target, unique_key) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.CompilationError(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
