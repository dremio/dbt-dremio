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

{% materialization udf, adapter='dremio' %}
  {%- set folder_name = config.get('folder_name', '') -%}
  {%- set target = folder_name ~ ('.' if folder_name else '') ~ this.identifier -%}

  {%- set parameter_list = config.get('parameter_list') -%}
  {%- set ret = config.get('returns') -%}

  {%- set create_sql -%}
CREATE OR REPLACE FUNCTION {{ target }}({{ parameter_list }})
RETURNS {{ ret }}

{{ sql }}
;
  {%- endset -%}

  {% call statement('main') -%}
    {{ create_sql }}
  {%- endcall %}

  {{ return({'relations': []}) }}
{% endmaterialization %}
