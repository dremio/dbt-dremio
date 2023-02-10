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

{%- macro get_quoted_csv_source_columns(source_name, table_name) -%}
  {%- if execute -%}
    {%- set source = graph.sources.values() | selectattr("source_name", "equalto", source_name) | selectattr("name", "equalto", table_name) | list | first -%}
    {# Got a hard time finding the following line !!! #}
    {%- for col_name, column in source.columns.items() -%}
      {%- if column.name in source.external.date_columns -%}
        {%- set date_format = source.external.date_format if source.external.date_format is defined else 'YYYY-MM-DD' -%}
        to_date(nullif(btrim({{ adapter.quote(column.name) }}), ''), '{{ date_format }}')
      {%- elif column.name in source.external.decimal_columns -%}
        {%- set decimal_separator = source.external.decimal_separator if source.external.decimal_separator is defined else ',' -%}
        {%- set decimals = source.external.decimals if source.external.decimals is defined else 2 -%}
        cast(cast(replace(nullif(btrim({{ adapter.quote(column.name) }}), ''), '{{ decimal_separator }}', '.') as double) as decimal(100, {{ decimals }}))
      {%- else -%}
        nullif(btrim({{ adapter.quote(column.name) }}), '')
      {%- endif -%} as {{ adapter.quote(column.name) }}
      {%- if not loop.last %}
      ,{%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{%- endmacro -%}
