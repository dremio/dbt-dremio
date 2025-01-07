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

  {% set reflection_name = config.get('name', validator=validation.any[basetring]) or 'Unnamed Reflection' %}
  {% set raw_reflection_type = config.get('reflection_type', validator=validation.any[basestring]) or 'raw' %}
  {% set raw_anchor = config.get('anchor', validator=validation.any[list, basestring]) %}
  {% set raw_external_target = config.get('external_target', validator=validation.any[list, basestring]) %}
  {% set identifier = model['alias'] %}
  {%- set display = config.get('display', validator=validation.any[list, basestring]) -%}
  {%- set dimensions = config.get('dimensions', validator=validation.any[list, basestring]) -%}
  {%- set date_dimensions = config.get('date_dimensions', validator=validation.any[list, basestring]) -%}
  {%- set measures = config.get('measures', validator=validation.any[list, basestring]) -%}
  {%- set computations = config.get('computations', validator=validation.any[list, basestring]) -%}
  {%- set partition_by = config.get('partition_by', validator=validation.any[basestring]) -%}
  {%- set partition_transform = config.get('partition_transform', validator=validation.any[basestring]) -%}
  {%- set partition_method = config.get('partition_method', validator=validation.any[basestring]) or 'striped' -%}
  {%- set distribute_by = config.get('distribute_by', validator=validation.any[basestring])-%}
  {%- set localsort_by = config.get('localsort_by', validator=validation.any[basestring]) -%}
  {%- set arrow_cache = config.get('arrow_cache') -%}

  {% set relation = this %}

  {% if measures is not none and computations is not none %}
    {% if measures | length != computations | length %}
      {% do exceptions.CompilationError("measures and computations should match in length") %}
    {%- endif -%}
  {%- endif %}

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
      identifier=identifier, schema=anchor.schema, database=anchor.database, type='materialized_view') -%}

  {%- set reflection_type = dbt_dremio_validate_get_reflection_type(raw_reflection_type) -%}
  {% if (reflection_type == 'raw' and display is none)
    or (reflection_type == 'aggregate' and (dimensions is none or measures is none or computations is none or partition_transform is none)) %}
    {% set columns = adapter.get_columns_in_relation(anchor) %}
    {% if reflection_type == 'raw' %}
      {% set display = columns | map(attribute='name') | list %}
    {% elif reflection_type == 'aggregate' %}
      {% if dimensions is none %}
        {% set dimensions = columns | rejectattr('dtype', 'in', ['decimal', 'float', 'double']) | map(attribute='name') | list %}
        {% set date_dimensions = columns | selectattr('dtype', 'in', ['timestamp']) | map(attribute='name') | list %}
      {% endif %}
      {% if measures is none %}
        {% set measures = columns | selectattr('dtype', 'in', ['decimal', 'float', 'double']) | map(attribute='name') | list %}
      {% endif %}
      {% if computations is none %}
        {% if measures is not none %}
          {% set computations = [] %}
          {% for measure in measures %}
            {% if measure in columns | selectattr('dtype', 'in', ['decimal', 'float', 'double']) | map(attribute='name') | list %}
              {% set _ = computations.append("SUM, COUNT") %}
            {% else %}
              {% set _ = computations.append("COUNT") %}
            {% endif %}
          {% endfor %}
        {% else %}
          {{ log("measures is null or undefined; not setting default computations.") }}
        {% endif %}
      {% endif %}
      {% if partition_transform is none %}
        {% if partition_by is not none %}
          {% set partition_transform = ['IDENTITY'] * (partition_by | length) %}
        {% else %}
          {{ log("partition_by is null or undefined; not setting default partition_transform.") }}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% call statement('main') -%}
    {{ create_reflection(reflection_name, reflection_type, anchor,
      display=display, dimensions=dimensions, date_dimensions=date_dimensions, measures=measures, computations=computations, partition_by=partition_by, partition_transform=partition_transform, partition_method=partition_method, distribute_by=distribute_by, localsort_by=localsort_by, arrow_cache=arrow_cache) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
