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

{#
/**
 * Parses a CTAS statement.
 * CREATE TABLE tblname [ (field1, field2, ...) ]
 *       [ (STRIPED, HASH, ROUNDROBIN) PARTITION BY (field1, field2, ..) ]
 *       [ DISTRIBUTE BY (field1, field2, ..) ]
 *       [ LOCALSORT BY (field1, field2, ..) ]
 *       [ STORE AS (opt1 => val1, opt2 => val3, ...) ]
 *       [ WITH SINGLE WRITER ]
 *       [ AS select_statement. ]
 */
#}

{% macro dremio__create_table_as(temporary, relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
     {{exceptions.warn("Model contracts are not enforced by dbt-dremio!")}}
  {% endif %}

  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create table {{ relation }}
  {{ partition_method() }} {{ config_cols("partition by") }}
  {{ config_cols("distribute by") }}
  {{ config_cols("localsort by") }}
  {{ store_as_clause() }}
  {{ single_writer_clause() }}
  as (
    {{ sql }}
  )
{%- endmacro -%}

{% macro store_as_clause() -%}
  {%- set options = format_clause_from_config() -%}
  {%- if options is not none -%}
  store as ( {{ options }} )
  {%- endif %}
{%- endmacro -%}

{%- macro single_writer_clause() -%}
  {%- set single_writer = config.get('single_writer', validator=validation.any[boolean]) -%}
  {%- if single_writer is not none and single_writer -%}
    with single writer
  {%- endif -%}
{%- endmacro -%}
