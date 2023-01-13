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
ALTER TABLE tblname
ADD RAW REFLECTION name
USING
DISPLAY (field1, field2)
[ DISTRIBUTE BY (field1, field2, ..) ]
[ (STRIPED, CONSOLIDATED) PARTITION BY (field1, field2, ..) ]
[ LOCALSORT BY (field1, field2, ..) ]
[ ARROW CACHE ]

ALTER TABLE tblname
ADD AGGREGATE REFLECTION name
USING
DIMENSIONS (field1, field2)
MEASURES (field1, field2)
[ DISTRIBUTE BY (field1, field2, ..) ]
[ (STRIPED, CONSOLIDATED) PARTITION BY (field1, field2, ..) ]
[ LOCALSORT BY (field1, field2, ..) ]
[ ARROW CACHE ]

ALTER TABLE tblname
ADD EXTERNAL REFLECTION name
USING target
#}

{%- macro create_reflection(reflection_type, anchor, reflection, external_target=none,
  display=none, dimensions=none, by_day_dimensions=none, measures=none) %}
  alter dataset {{ anchor }}
    create {{ reflection_type }} reflection {{ reflection.include(database=False, schema=False) }}
    using
    {%- if reflection_type == 'raw' %}
      {{ display_clause(display) }}
    {%- elif reflection_type == 'aggregate' %}
      {{ dimensions_clause(dimensions=dimensions, by_day_dimensions=by_day_dimensions) }}
      {{ measures_clause(measures) }}
    {%- else -%}
      {{ external_target }}
    {% endif -%}
    {%- if reflection_type in ['raw', 'aggregate'] %}
      {{ partition_method() }} {{ config_cols("partition by") }}
      {{ config_cols("localsort by") }}
      {{ config_cols("distribute by") }}
      {{ arrow_cache_clause() }}
    {%- endif -%}
{% endmacro -%}

{%- macro display_clause(display=none) %}
  {%- set cols = config.get('display', validator=validation.any[list, basestring]) or display -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    display (
    {%- for item in cols -%}
      {{ adapter.quote(item) }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{% endmacro -%}

{%- macro dimensions_clause(dimensions=none, by_day_dimensions=none) %}
  {%- set cols = config.get('dimensions', validator=validation.any[list, basestring]) or dimensions -%}
  {%- set by_day_cols = config.get('by_day_dimensions', validator=validation.any[list, basestring]) or by_day_dimensions -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {%- if by_day_cols is string -%}
      {%- set by_day_cols = [by_day_cols] -%}
    {%- endif -%}
    dimensions (
    {%- for item in cols -%}
      {{ adapter.quote(item) ~ (' by day' if item in by_day_cols else "") }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{% endmacro -%}

{%- macro measures_clause(measures=none) %}
  {%- set cols = config.get('measures', validator=validation.any[list, basestring]) or measures -%}
  {%- set comp_cols = config.get('computations', validator=validation.any[list, basestring]) or [] -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {%- if comp_cols is string -%}
      {%- set comp_cols = [comp_cols] -%}
    {%- endif -%}
    measures (
    {%- for item in cols -%}
      {%- set computations = (' (' ~ comp_cols[loop.index0] ~ ')')
      if loop.index0 < comp_cols | length and comp_cols[loop.index0] is not none else '' -%}
      {{ adapter.quote(item) ~ computations }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{% endmacro -%}

{%- macro arrow_cache_clause() -%}
  {%- set arrow_cache = config.get('arrow_cache', validator=validation.any[boolean]) -%}
  {%- if arrow_cache is not none and arrow_cache -%}
    arrow cache
  {%- endif -%}
{% endmacro -%}
