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

{%- macro external_query(sql) -%}
  {%- set source = validate_external_query() -%}
  {%- if source is not none -%}
    {%- set escaped_sql = sql | replace("'", "''") -%}
    {%- set result -%}
      select *
      from table({{ builtins.source(source[0], source[1]).include(schema=false, identifier=false) }}.external_query('{{ escaped_sql }}'))
    {%- endset -%}
  {%- else -%}
    {%- set result = sql -%}
  {%- endif -%}
  {{ return(result) }}
{%- endmacro -%}

{%- macro validate_external_query() -%}
  {%- set external_query = config.get('external_query', validator=validation.any[boolean]) or false -%}
  {%- if external_query -%}
    {%- if model.refs | length == 0 and model.sources | length > 0 -%}
      {%- set source_names = [] -%}
      {%- for source in model.sources -%}
        {%- do source_names.append(source[0]) if source[0] not in source_names -%}
      {% endfor %}
      {%- if source_names | length == 1 -%}
        {{ return(model.sources[0]) }}
      {%- else -%}
        {% do exceptions.CompilationError("Invalid external query configuration: awaiting one single source name among all source dependencies") %}
      {%- endif -%}
    {%- else -%}
      {% do exceptions.CompilationError("Invalid external query: awaiting only source dependencies") %}
    {%- endif -%}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{%- endmacro -%}
