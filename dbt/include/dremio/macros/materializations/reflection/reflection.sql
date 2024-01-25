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

{% materialization reflection, adapter='dremio' %}
  {%- if not  var('dremio:reflections_enabled', default=false)  -%}
    {% do exceptions.CompilationError("reflections are disabled, set 'dremio:reflections_enabled' variable to true to enable them") %}
  {%- endif -%}

  {% set raw_reflection_type = config.get('reflection_type', validator=validation.any[basestring]) or 'raw' %}
  {% set raw_anchor = config.get('anchor', validator=validation.any[list, basestring]) %}
  {% set raw_external_target = config.get('external_target', validator=validation.any[list, basestring]) %}
  {% set identifier = model['alias'] %}
  {%- set display = config.get('display', validator=validation.any[list, basestring]) -%}
  {%- set dimensions = config.get('dimensions', validator=validation.any[list, basestring]) -%}
  {%- set measures = config.get('measures', validator=validation.any[list, basestring]) -%}

  {% if model.refs | length + model.sources | length == 1 %}
    {% if model.refs | length == 1 %}
      {% set anchor = ref(model.refs[0]['name']) %}
    {% else %}
      {% set anchor = source(model.sources[0][0], model.sources[0][1]) %}
    {% endif %}
  {% elif model.refs | length + model.sources | length > 1 %}
    {% if raw_anchor is not none %}
      {% if raw_anchor is string %}
        {% set raw_anchor = [raw_anchor] %}
      {% endif %}
      {% if raw_anchor | length == 1 %}
        {% set anchor = ref(raw_anchor[0]) %}
      {% elif raw_anchor | length == 2 %}
        {% set anchor = source(raw_anchor[0], raw_anchor[1]) %}
      {% endif %}
    {% endif %}
    {% if raw_external_target is not none %}
      {% if raw_external_target is string %}
        {% set raw_external_target = [raw_external_target] %}
      {% endif %}
      {% if raw_external_target | length == 1 %}
        {% set external_target = ref(raw_external_target[0]) %}
      {% elif raw_external_target | length == 2 %}
        {% set external_target = source(raw_external_target[0], raw_external_target[1]) %}
      {% endif %}
    {% endif %}
  {% endif %}

  {%- set old_relation = adapter.get_relation(database=anchor.database, schema=anchor.schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=anchor.schema, database=anchor.database, type='materializedview') -%}

  {%- set reflection_type = dbt_dremio_validate_get_reflection_type(raw_reflection_type) -%}
  {% if (reflection_type == 'raw' and display is none)
    or (reflection_type == 'aggregate' and (dimensions is none or measures is none)) %}
    {% set columns = adapter.get_columns_in_relation(anchor) %}
    {% if reflection_type == 'raw' %}
      {% set display = columns | map(attribute='name') | list %}
    {% elif reflection_type == 'aggregate' %}
      {% if dimensions is none %}
        {% set dimensions = columns | rejectattr('dtype', 'in', ['decimal', 'float', 'double']) | map(attribute='name') | list %}
        {% set by_day_dimensions = columns | selectattr('dtype', 'in', ['timestamp']) | map(attribute='name') | list %}
      {% endif %}
      {% if measures is none %}
        {% set measures = columns | selectattr('dtype', 'in', ['decimal', 'float', 'double']) | map(attribute='name') | list %}
      {% endif %}
    {% endif %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {{ drop_reflection_if_exists(anchor, old_relation) }}
  -- build model
  {% call statement('main') -%}
    {{ create_reflection(reflection_type, anchor, target_relation, external_target,
      display=display, dimensions=dimensions, by_day_dimensions=by_day_dimensions, measures=measures) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
