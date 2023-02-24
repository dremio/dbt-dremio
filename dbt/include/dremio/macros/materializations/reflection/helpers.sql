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

{% macro drop_reflection_if_exists(relation, reflection) %}
  {% if reflection is not none and reflection.type == 'materializedview' %}
    {% call statement('drop reflection') -%}
      alter dataset {{ relation }}
        drop reflection {{ reflection.include(database=False, schema=False) }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

{% macro dbt_dremio_validate_get_reflection_type(raw_reflection_type) %}
  {% set accepted_types = ['raw', 'aggregate', 'external'] %}
  {% set invalid_reflection_type_msg -%}
    Invalid reflection type provided: {{ raw_reflection_type }}
    Expected one of: {{ accepted_types | join(', ') }}
  {%- endset %}
  {% if raw_reflection_type not in accepted_types %}
    {% do exceptions.CompilationError(invalid_reflection_type_msg) %}
  {% endif %}
  {% do return(raw_reflection_type) %}
{% endmacro %}
