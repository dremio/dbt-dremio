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

{% macro dremio__handle_existing_table(full_refresh, old_relation) %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ exceptions.raise_not_implemented('Inside a dremio home space, a model cannot change from table to view materialization; please drop the table in the UI') }}
{% endmacro %}

{# ALTER VDS <dataset> SET ENABLE_DEFAULT_REFLECTION = TRUE | FALSE #}

{% macro enable_default_reflection() %}
  {%- set enable_default_reflection = config.get('enable_default_reflection', validator=validation.any[boolean]) -%}
  {%- if enable_default_reflection is not none -%}
    {% call statement('enable_default_reflection') -%}
      alter vds {{ this }} set enable_default_reflection = {{ enable_default_reflection }}
    {%- endcall %}
  {%- endif -%}
{% endmacro %}
