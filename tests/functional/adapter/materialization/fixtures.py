create_seed_sql = """
create table {database}.{schema}.seed (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP
);
"""

create_snapshot_expected_sql = """
create table {database}.{schema}.snapshot_expected (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),

    -- snapshotting fields
    updated_at TIMESTAMP,
    test_valid_from TIMESTAMP,
    test_valid_to   TIMESTAMP,
    test_scd_id     VARCHAR,
    test_updated_at TIMESTAMP
);
"""


seed_insert_sql = """
-- seed inserts
--  use the same email for two users to verify that duplicated check_cols values
--  are handled appropriately
insert into {database}.{schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28'),
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15'),
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30'),
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51'),
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38'),
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11'),
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46'),
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56'),
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16'),
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49'),
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48'),
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09'),
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19'),
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44'),
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21'),
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38'),
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20'),
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27'),
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06'),
(20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', '2016-08-21 10:35:19');
"""


populate_snapshot_expected_sql = """
-- populate snapshot table
insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    cast(null as timestamp) as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || cast(updated_at as varchar)) as test_scd_id
from {database}.{schema}.seed;
"""

populate_snapshot_expected_valid_to_current_sql = """
-- populate snapshot table
insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    cast('2099-12-31' as timestamp) as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || cast(updated_at as varchar)) as test_scd_id
from {database}.{schema}.seed;
"""

snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
        )
    }}

    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""

snapshots_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

snapshots_no_column_names_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
"""

ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""


invalidate_seed_sql = """
-- update records 11 - 21. Change email and updated_at field
update {database}.{schema}.seed set
    updated_at = TIMESTAMPADD(SQL_TSI_HOUR, 1, updated_at),
    email      =  case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20
"""

invalidate_snapshot_sql = """
-- invalidate records 11 - 21
update {database}.{schema}.snapshot_expected set
    test_valid_to   = TIMESTAMPADD(SQL_TSI_HOUR, 1, updated_at)
where id >= 10 and id <= 20
"""

update_sql = """
-- insert v2 of the 11 - 21 records

insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    cast(null as timestamp) as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || cast(updated_at as varchar)) as test_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20;
"""

# valid_to_current fixtures

snapshots_valid_to_current_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "cast('2099-12-31' as timestamp)"
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

update_with_current_sql = """
-- insert v2 of the 11 - 21 records

insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    cast('2099-12-31' as timestamp) as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || cast(updated_at as varchar)) as test_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20;
"""
