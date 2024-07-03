from tests.utils.util import SOURCE

MODEL_PRE_HOOK = f"""
   insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    'start',
    '{{{{ target.dbname }}}}',
    '{{{{ target.host }}}}',
    '{{{{ target.name }}}}',
    '{SOURCE}.{{{{ target.schema }}}}',
    '{{{{ target.type }}}}',
    '{{{{ target.user }}}}',
    '{{{{ target.get("pass", "") }}}}',
    {{{{ target.threads }}}},
    '{{{{ run_started_at }}}}',
    '{{{{ invocation_id }}}}'
   )
"""

MODEL_POST_HOOK = f"""
   insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    'end',
    '{{{{ target.dbname }}}}',
    '{{{{ target.host }}}}',
    '{{{{ target.name }}}}',
    '{SOURCE}.{{{{ target.schema }}}}',
    '{{{{ target.type }}}}',
    '{{{{ target.user }}}}',
    '{{{{ target.get("pass", "") }}}}',
    {{{{ target.threads }}}},
    '{{{{ run_started_at }}}}',
    '{{{{ invocation_id }}}}'
   )
"""

macros_missing_column = """
{% macro export_table_check() %}

    {% set table = 'test_column' %}

    {% set query %}
        SELECT column_name
        FROM {{ref(table)}}
        LIMIT 1
    {% endset %}

    {%- if flags.WHICH in ('run', 'build') -%}
        {% set results = run_query(query) %}
        {% if execute %}
            {%- if results.rows -%}
                {{ exceptions.raise_compiler_error("ON_RUN_START_CHECK_NOT_PASSED: Data already exported. DBT Run aborted.") }}
            {% else -%}
                {{ log("No data found in " ~ table ~ " for current day and runtime region. Proceeding...", true) }}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
"""

models__missing_column = """
select 1 as col
"""

macros__before_and_after = f"""
{{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}}

   insert into {SOURCE}.{{{{ target.schema }}}}.on_run_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    '{{{{ state }}}}',
    '{{{{ target.dbname }}}}',
    '{{{{ target.host }}}}',
    '{{{{ target.name }}}}',
    '{SOURCE}.{{{{ target.schema }}}}',
    '{{{{ target.type }}}}',
    '{{{{ target.user }}}}',
    '{{{{ target.get("pass", "") }}}}',
    {{{{ target.threads }}}},
    '{{{{ run_started_at }}}}',
    '{{{{ invocation_id }}}}'
   )

{{% endmacro %}}
"""

macros__hook = """
{% macro hook() %}
  select 1
{% endmacro %}
"""

models__hooks = """
select 1 as id
"""

models__hooks_configured = f"""
{{{{
    config({{
        "pre_hook": "\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id
            ) VALUES (\
                'start',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'\
        )",
        "post-hook": "\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id
            ) VALUES (\
                'end',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'\
            )"
    }})
}}}}

select 3 as id
"""

models__hooks_error = f"""
{{{{
    config({{
        "pre_hook": "\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id
            ) VALUES (\
                'start',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'
        )",
        "pre-hook": "\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id
            ) VALUES (\
                'start',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'
        )",
        "post-hook": "\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id
            ) VALUES (\
                'end',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'\
            )"
    }})
}}}}

select 3 as id
"""

models__hooks_kwargs = f"""
{{{{
    config(
        pre_hook="\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id
            ) VALUES (\
                'start',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'\
        )",
        post_hook="\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id\
            ) VALUES (\
                'end',\
                '{{{{ target.dbname }}}}',\
                '{{{{ target.host }}}}',\
                '{{{{ target.name }}}}',\
                '{SOURCE}.{{{{ target.schema }}}}',\
                '{{{{ target.type }}}}',\
                '{{{{ target.user }}}}',\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}',\
                {{{{ target.threads }}}},\
                '{{{{ run_started_at }}}}',\
                '{{{{ invocation_id }}}}'\
            )"
    )
}}}}

select 3 as id
"""

models__hooked = f"""
{{{{
    config({{
        "pre_hook": "\
            insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook select
                test_state,
                '{{{{ target.dbname }}}}' as target_dbname,\
                '{{{{ target.host }}}}' as target_host,\
                '{{{{ target.name }}}}' as target_name,\
                '{SOURCE}.{{{{ target.schema }}}}' as target_schema,\
                '{{{{ target.type }}}}' as target_type,\
                '{{{{ target.user }}}}' as target_user,\
                '{{{{ target.get(\\"pass\\", \\"\\") }}}}' as target_pass,\
                {{{{ target.threads }}}} as target_threads,\
                '{{{{ run_started_at }}}}' as run_started_at,\
                '{{{{ invocation_id }}}}' as invocation_id
                from {{{{ ref('pre') }}}}\
                "
    }})
}}}}
select 1 as id
"""

models__post = """
select 'end' as test_state
"""

models__pre = """
select 'start' as test_state
"""

snapshots__test_snapshot = f"""
{{% snapshot example_snapshot %}}
{{{{
    config(target_schema=schema, unique_key='a', strategy='check', check_cols='all')
}}}}
select * from {{{{ ref('example_seed') }}}}
{{% endsnapshot %}}
"""

properties__seed_models = """
version: 2
seeds:
- name: example_seed
  columns:
  - name: new_col
    tests:
    - not_null
"""

properties__test_snapshot_models = """
version: 2
snapshots:
- name: example_snapshot
  columns:
  - name: new_col
    tests:
    - not_null
"""

seeds__example_seed_csv = """a,b,c
1,2,3
4,5,6
7,8,9
"""
