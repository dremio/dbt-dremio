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
ALTER PDS <PHYSICAL-DATASET-PATH> REFRESH METADATA
    [AVOID PROMOTION | AUTO PROMOTION]
    [FORCE UPDATE | LAZY UPDATE]
    [MAINTAIN WHEN MISSING | DELETE WHEN MISSING]

ALTER PDS <PHYSICAL-DATASET-PATH> FORGET METADATA

ALTER TABLE <TABLE> REFRESH METADATA
#}

{% macro refresh_metadata(relation, format='iceberg') -%}
  {%- if format != 'iceberg' -%}
    {% call statement('refresh_metadata') -%}
      {%- if format == 'parquet' -%}
        {{ alter_table_refresh_metadata(relation) }}
      {%- else -%}
        {{ alter_pds(relation, avoid_promotion=false, lazy_update=false) }}
      {%- endif -%}
    {%- endcall %}
  {%- endif -%}
{%- endmacro -%}

{% macro alter_table_refresh_metadata(table_relation) -%}
  alter table {{ table_relation }} refresh metadata
{%- endmacro -%}

{% macro alter_pds(table_relation, avoid_promotion=True, lazy_update=True, delete_when_missing=True, forget_metadata=False) -%}
  alter pds {{ table_relation }} refresh metadata
  {% if forget_metadata %}
    forget metadata
  {%- else -%}
    {%- if avoid_promotion %}
      avoid promotion
    {%- else %}
      auto promotion
    {%- endif %}
    {%- if lazy_update %}
      lazy update
    {%- else %}
      force update
    {%- endif %}
    {%- if delete_when_missing %}
      delete when missing
    {%- else %}
      maintain when missing
    {%- endif -%}
  {%- endif %}
{%- endmacro -%}
