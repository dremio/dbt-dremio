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

{%- macro trim_varchar_columns(column_names, trimtext=none) -%}
  {%- if column_names is string -%}
    {%- set column_names = [column_names] -%}
  {% endif -%}
  {%- set result = [] -%}
  {%- for column_name in column_names -%}
    {%- set sql -%}
      nullif(btrim({{ adapter.quote(column_name) }}
        {%- if trimtext -%}, '{{ trimtext }}'{%- endif -%}
      ), '')
    {%- endset -%}
    {%- do result.append(
      {
        'name': column_name
        ,'sql': sql
      }
    ) -%}
  {%- endfor -%}
  {{ return(result) }}
{%- endmacro -%}

{%- macro to_date_varchar_columns(column_names, format='YYYY-MM-DD') -%}
  {%- if column_names is string -%}
    {%- set column_names = [column_names] -%}
  {% endif -%}
  {%- set result = [] -%}
  {%- for column_name in column_names -%}
    {%- set sql -%}
      to_date(nullif(btrim({{ adapter.quote(column_name) }}), ''), '{{ format }}')
    {%- endset -%}
    {%- do result.append(
      {
        'name': column_name
        ,'sql': sql
      }
    ) -%}
  {%- endfor -%}
  {{ return(result) }}
{%- endmacro -%}

{%- macro to_decimal_varchar_columns(column_names, decimal_separator=',', decimals=2) -%}
  {%- if column_names is string -%}
    {%- set column_names = [column_names] -%}
  {% endif -%}
  {%- set result = [] -%}
  {%- for column_name in column_names -%}
    {%- set sql -%}
      cast(cast(replace(nullif(btrim({{ adapter.quote(column_name) }}), ''), '{{ decimal_separator }}', '.') as double) as decimal(100, {{ decimals }}))
    {%- endset -%}
    {%- do result.append(
      {
        'name': column_name
        ,'sql': sql
      }
    ) -%}
  {%- endfor -%}
  {{ return(result) }}
{%- endmacro -%}

{%- macro get_quoted_csv_sql_columns(sql_columns) -%}
  {%- for col in sql_columns -%}
    {{ col['sql'] }} as {{ adapter.quote(col['name']) }}
    {% if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{%- endmacro -%}
