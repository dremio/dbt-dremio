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

{% macro dremio__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}
    {{ adapter.drop_relation(tmp_relation) }}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro dremio__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro drop_relation_with_branch(relation, branch=none) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}{%- if branch is not none %} at branch {{ branch }}{%- endif %}
  {%- endcall %}
{% endmacro %}

{% macro create_branch_statement(relation, branch) -%}
  {%- set nessie_ref = config.get('nessie_ref', validator=validation.any[string]) -%}
  {%- if execute -%}
    {%- set create_branch_sql -%}
      create branch if not exists {{ branch }}{% if nessie_ref is not none %} at ref {{ nessie_ref }}{% endif %} in {{ relation.database }}
    {%- endset -%}
    {%- do run_query(create_branch_sql) -%}
  {%- endif -%}
{%- endmacro %}

{% macro get_relation_at_branch(database, schema, identifier, branch) -%}
  {%- if execute -%}
    {# First, check if the branch exists #}
    {%- set branch_sql -%}
      show branches in {{ database }}
    {%- endset -%}
    {%- set branch_result = run_query(branch_sql) -%}
    {%- set branch_exists = False -%}
    {%- for row in branch_result.rows -%}
      {%- if row[0] == branch -%}
        {%- set branch_exists = True -%}
      {%- endif -%}
    {%- endfor -%}
    
    {# If branch exists, check if table exists in that branch #}
    {%- set found = False -%}
    {%- if branch_exists -%}
      {%- set table_sql -%}
        show tables in {{ database }}.{{ schema }} at branch {{ branch }}
      {%- endset -%}
      {%- set table_result = run_query(table_sql) -%}
      {%- if table_result.rows | length > 0 -%}
        {%- for row in table_result.rows -%}
          {%- if row[1] == identifier -%}
            {%- set found = True -%}
          {%- endif -%}
        {%- endfor -%}
      {%- endif -%}
    {%- endif -%}
    {{ found }}
  {%- else -%}
    {{ False }}
  {%- endif -%}
{%- endmacro %}

{% macro dremio__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation1/2 - create to_relation from from_relation') -%}
    {{ get_create_table_as_sql(temporary=False, relation=to_relation, sql="select * from " ~ from_relation)}}
  {%- endcall %}
  {% call statement('rename_relation2/2 - drop from_relation') -%}
    DROP TABLE {{from_relation}}
  {%- endcall %}
{% endmacro %}
