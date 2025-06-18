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


{% macro dremio__snapshot_merge_sql(target, source, insert_cols) -%}
  {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  merge into {{ target }} as DBT_INTERNAL_DEST
    using (
      select * from {{ source }}
      where dbt_change_type in ('update', 'delete', 'insert')
    ) as DBT_INTERNAL_SOURCE
    on (DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }} = DBT_INTERNAL_DEST.{{ columns.dbt_scd_id }}
        and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        {%- if config.get("dbt_valid_to_current") %}
        and (
          DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }}
          or DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null
        )
        {%- else %}
        and DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null
        {%- endif %})

    when matched then update set {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}

    when not matched then insert ({{ insert_cols_csv }})
      values
      (
        {%- for column_name in insert_cols -%}
          DBT_INTERNAL_SOURCE.{{ column_name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
      )

{% endmacro %}
