
drop table if exists dbt_test_source.{schema}.on_run_hook;

create table dbt_test_source.{schema}.on_run_hook (
    test_state       VARCHAR, -- start|end
    target_dbname    VARCHAR,
    target_host      VARCHAR,
    target_name      VARCHAR,
    target_schema    VARCHAR,
    target_type      VARCHAR,
    target_user      VARCHAR,
    target_pass      VARCHAR,
    target_threads   INT,
    run_started_at   VARCHAR,
    invocation_id    VARCHAR
);
