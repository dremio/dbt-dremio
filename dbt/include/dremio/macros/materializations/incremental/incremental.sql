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

{% materialization incremental, adapter='dremio' -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%} 
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key', validator=validation.any[list, basestring]) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(raw_on_schema_change) -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparison later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if existing_relation is none %}
    {% set build_sql = get_create_table_as_sql(False, target_relation, external_query(sql)) %}
  {% elif full_refresh_mode %}
    {% set build_sql = get_create_table_as_sql(False, intermediate_relation, external_query(sql)) %}
    {% set need_swap = true %}
  {% else %}
    {% call statement('temp') -%}
       {{ create_table_as(True, temp_relation, external_query(sql)) }}
    {%- endcall %}
    {% do to_drop.append(temp_relation) %}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    -- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = dremio__get_columns_in_relation(existing_relation) %}
    {% endif %}

    -- Get the incremental_strategy, the macro to use for the strategy, and build the sql
    {%- set incremental_strategy = config.get('incremental_strategy', validator=validation.any[basestring]) or 'append' -%}
    {%- set raw_file_format = config.get('format', validator=validation.any[basestring]) or 'iceberg' -%}
    {%- set file_format = dbt_dremio_validate_get_file_format(raw_file_format) -%}
    {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
    {%- set strategy = dbt_dremio_validate_get_incremental_strategy(incremental_strategy) -%}
    {%- set raw_on_schema_change = config.get('on_schema_change', validator=validation.any[basestring]) or 'ignore' -%}
    {% set build_sql = dbt_dremio_get_incremental_sql(strategy, intermediate_relation, target_relation, dest_columns, unique_key) %}
    
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {{ refresh_metadata(target_relation, raw_file_format) }}

  {{ apply_twin_strategy(target_relation) }}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}
  
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]})}}

{%- endmaterialization %}
