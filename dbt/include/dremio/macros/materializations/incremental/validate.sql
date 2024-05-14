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

{% macro dbt_dremio_validate_get_file_format(raw_file_format) %}
  {#-- Validate the file format #}

  {% set accepted_formats = ['iceberg'] %}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.CompilationError(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{% endmacro %}

{% macro dbt_dremio_validate_get_incremental_strategy(raw_strategy) %}
  {#-- Validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append, merge'
  {%- endset %}

  {% if raw_strategy not in ['append', 'merge'] %}
    {% do exceptions.CompilationError(invalid_strategy_msg) %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}
