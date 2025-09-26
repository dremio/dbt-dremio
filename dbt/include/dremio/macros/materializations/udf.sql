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
  {%- set identifier = model['alias'] -%}
  
  {%- set udf_database = config.get('database', validator=validation.any[basestring]) or database -%}
  {%- set udf_schema = config.get('schema', validator=validation.any[basestring]) or schema -%}
  
  {# Create proper relation object for UDF #}
  {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=udf_schema, 
      database=udf_database,
      type='view') -%}

  {%- set parameter_list = config.get('parameter_list', validator=validation.any[basestring]) -%}
  {%- set ret = config.get('returns', validator=validation.any[basestring]) -%}

  {# Validate required configurations #}
  {%- if parameter_list is none or parameter_list == '' -%}
    {{ exceptions.raise_compiler_error("UDF materialization requires 'parameter_list' configuration specifying the function parameters (e.g., parameter_list='x INT, y INT').") }}
  {%- endif -%}
  
  {%- if ret is none -%}
    {{ exceptions.raise_compiler_error("UDF materialization requires 'returns' configuration specifying the return type (e.g., returns='INT').") }}
  {%- endif -%}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {# Check for existing relation and handle conflicts #}
  {%- set old_relation = adapter.get_relation(database=udf_database, schema=udf_schema, identifier=identifier) -%}
  {%- if old_relation is not none -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif -%}

  {# Build model #}
  {% call statement('main') -%}
    create or replace function {{ target_relation }}({{ parameter_list }})
    returns {{ ret }}
    {{ sql }}
  {%- endcall %}

  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
