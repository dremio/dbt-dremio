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

{%- macro create_reflection(reflection_name, reflection_type, anchor,
  display=none, dimensions=none, date_dimensions=none, measures=none, computations=none, partition_by=none, partition_transform=none, partition_method=none, distribute_by=none, localsort_by=none, arrow_cache=false) %}

  {%- if reflection_type == 'raw' %}
    {% set reflection_type = 'RAW' %}
  {%- elif reflection_type == 'aggregate' %}
    {% set reflection_type = 'AGGREGATION' %}
  {%- else -%}
    {% do exceptions.CompilationError("invalid reflection type") %}
  {%- endif -%}

  {% do adapter.create_reflection(reflection_name, reflection_type, anchor, display, dimensions, date_dimensions, measures, computations, partition_by, partition_transform, partition_method, distribute_by, localsort_by, arrow_cache) %}

  SELECT 1
{% endmacro -%}