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

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set tmp_relation = adapter.get_relation(database=database, schema=schema, identifier=tmp_identifier) -%}
  {%- set target_tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set raw_strategy = config.get('incremental_strategy', validator=validation.any[basestring]) or 'append' -%}
  {%- set raw_file_format = config.get('format', validator=validation.any[basestring]) or 'iceberg' -%}
  {%- set file_format = dbt_dremio_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_dremio_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  {%- set unique_key = config.get('unique_key', validator=validation.any[list, basestring]) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set raw_on_schema_change = config.get('on_schema_change', validator=validation.any[basestring]) or 'ignore' -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(raw_on_schema_change) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {% if old_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, external_query(sql)) %}
  {% elif full_refresh_mode %}
    {% do adapter.drop_relation(old_relation) %}
    {% set build_sql = create_table_as(False, target_relation, external_query(sql)) %}
  {% else %}
    {% if tmp_relation is not none %}
      {{ adapter.drop_relation(tmp_relation) }}
    {% endif %}
    {{ run_query(create_table_as(True, target_tmp_relation, external_query(sql))) }}
    {{ process_schema_changes(on_schema_change, target_tmp_relation, old_relation) }}
    {% set build_sql = dbt_dremio_get_incremental_sql(strategy, target_tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% if not(old_relation is none or full_refresh_mode) %}
    {{ adapter.drop_relation(target_tmp_relation) }}
  {% endif %}

  {{ refresh_metadata(target_relation, raw_file_format) }}

  {{ apply_twin_strategy(target_relation) }}

  {% do persist_docs(target_relation, model) %}
  
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{%- endmaterialization %}
