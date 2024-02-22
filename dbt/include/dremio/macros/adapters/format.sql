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
input/output formats

"type": String ['text', 'json', 'arrow', 'parquet', 'iceberg']

for 'text' :
"fieldDelimiter": String,
"lineDelimiter": String,
"quote": String,
"comment": String,
"escape": String,
"skipFirstLine": Boolean,
"extractHeader": Boolean,
"trimHeader": Boolean,
"autoGenerateColumnNames": Boolean

for 'json' :
"prettyPrint" : Boolean

input only formats

"type": String ['delta', 'excel']

for 'excel' :
"xls": Boolean
"sheetName": String,
"extractHeader": Boolean,
"hasMergedCells": Boolean

#}

{% macro format_clause_from_config() -%}
  {%- set key_map = {'format':'type'
    ,'field_delimiter':'fieldDelimiter'
    ,'line_delimiter':'lineDelimiter'
    ,'skip_first_line':'skipFirstLine'
    ,'extract_header':'extractHeader'
    ,'trim_header':'trimHeader'
    ,'auto_generated_column_names':'autoGenerateColumnNames'
    ,'pretty_print':'prettyPrint'} -%}
  {%- set options = [] -%}
  {%- set format = config.get('format', validator=validation.any[basestring])-%}
  {%- if format in ['text', 'json', 'arrow', 'parquet', 'iceberg'] -%}
    {%- do options.append("type=>'" ~ format ~ "'") -%}
    {%- if format == 'text' -%}
      {%- for key in ['field_delimiter', 'line_delimiter', 'quote', 'comment', 'escape'] -%}
        {%- set value = config.get(key, validator=validation.any[basestring]) -%}
        {%- set key = key_map[key] or key -%}
        {%- if value is not none -%}
          {%- do options.append(key ~ "=>'" ~ value ~ "'") -%}
        {%- endif -%}
      {%- endfor -%}
      {%- for key in ['skip_first_line', 'extract_header', 'trim_header', 'auto_generated_column_names'] -%}
        {%- set value = config.get(key, validator=validation.any[boolean]) -%}
        {%- set key = key_map[key] or key -%}
        {%- if value is not none -%}
          {%- do options.append(key ~ "=>" ~ value) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- elif format == 'json' -%}
      {%- set key = 'pretty_print' -%}
      {%- set value = config.get(key, validator=validation.any[boolean]) -%}
      {%- set key = key_map[key] or key -%}
      {%- if value is not none -%}
        {%- do options.append(key ~ "=>" ~ value) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {{ return((options | join(', ')) if options | length > 0 else none) }}
{%- endmacro -%}

{%- macro format_clause_from_node(config) -%}
{%- set key_map = {'format':'type'
  ,'field_delimiter':'fieldDelimiter'
  ,'line_delimiter':'lineDelimiter'
  ,'skip_first_line':'skipFirstLine'
  ,'extract_header':'extractHeader'
  ,'trim_header':'trimHeader'
  ,'auto_generated_column_names':'autoGenerateColumnNames'
  ,'pretty_print':'prettyPrint'
  ,'sheet_name':'sheetName'
  ,'has_merged_cells':'hasMergedCells'} -%}
  {%- set options = [] -%}
  {%- set format = config['format'] -%}
  {%- if format is defined and format is string and format in ['text', 'json', 'arrow', 'parquet', 'avro', 'excel', 'delta'] -%}
    {%- do options.append("type=>'" ~ format ~ "'") -%}
  {%- endif -%}
  {%- if format == 'text' -%}
    {%- for key in ['field_delimiter', 'line_delimiter', 'quote', 'comment', 'escape'] -%}
      {%- set value = config[key] -%}
      {%- if value is defined and value is string -%}
        {%- set key = key_map[key] or key -%}
        {%- do options.append(key ~ "=>'" ~ value ~ "'") -%}
      {%- endif -%}
    {%- endfor -%}
    {%- for key in ['skip_first_line', 'extract_header', 'trim_header', 'auto_generated_column_names'] -%}
      {%- set value = config[key] -%}
      {%- if value is defined and value is boolean -%}
        {%- set key = key_map[key] or key -%}
        {%- do options.append(key ~ "=>" ~ value) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- elif format == 'json' -%}
    {%- set key = 'pretty_print' -%}
    {%- set value = config[key] -%}
    {%- if value is defined and value is boolean -%}
      {%- set key = key_map[key] or key -%}
      {%- do options.append(key ~ "=>" ~ value) -%}
    {%- endif -%}
  {%- elif format == 'excel' -%}
    {%- for key in ['sheet_name'] -%}
      {%- set value = config[key] -%}
      {%- if value is defined and value is string -%}
        {%- set key = key_map[key] or key -%}
        {%- do options.append(key ~ "=>'" ~ value ~ "'") -%}
      {%- endif -%}
    {%- endfor -%}
    {%- for key in ['xls', 'extract_header', 'has_merged_cells'] -%}
      {%- set value = config[key] -%}
      {%- if value is defined and value is boolean -%}
        {%- set key = key_map[key] or key -%}
        {%- do options.append(key ~ "=>" ~ value) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return((options | join(', ')) if options | length > 0 else none) }}
{%- endmacro -%}

{% macro render_with_format_clause(target_table) %}
  {%- set options = format_clause_from_config() -%}
  {% if options is not none -%}
    table(
  {%- endif %}
  {{ target_table }}
  {%- if options is not none -%}
    ( {{ options }} ))
  {%- endif -%}
{% endmacro %}
