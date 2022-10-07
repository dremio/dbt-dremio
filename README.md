![dbt-dremio](https://resumo.cloud/wp-content/uploads/2021/07/modelo-imagem-rc-16-1.png)

> *This project is developed during my spare time, along side my lead dev position at [MAIF-VIE](http://www.maif.fr), and aims to provide a competitive alternative solution for our current ETL stack.*

# dbt-dremio
[dbt](https://www.getdbt.com/)'s adapter for [dremio](https://www.dremio.com/), v1.1.0b

If you are reading this documentation, I assume you already know well both dbt and dremio. Please refer to their respective documentation.

# Installation
dbt dependencies :
 - dbt-core=1.1.x,
 - pyodbc>=4.0.27

dremio dependency :
 - latest Dremio's Flight SQL ODBC driver
 - Dremio >= 21.0.0
 - `dremio.iceberg.enabled`, `dremio.iceberg.ctas.enabled` and `dremio.execution.support_unlimited_splits` enabled

os dependency :
- Windows OS
- Linux
- Not currently supported: Apple Silicon MacOS (current limitation on Flight SQL ODBC driver)

`pip install dbt-dremio`

# Relation types
In dbt's world, A dremio relation can be either a `view` or a `table`. A dremio reflection - a dataset materialization with a refresh policy - will be mapped to a dbt `materializedview` relation.

# Databases
As Dremio is a federation tool, dbt's queries can span locations and so, in dremio's adapter, "databases" are paramount.
There are three kinds of dataset locations : external sources, datalakes and spaces. Sources are input locations, datalakes are both input and output locations and spaces can only contains views, with exceptions :

location|can create table| can drop table |can create/drop view
-|-|-|-
external source|no|no|no
datalake|if CTAS (`CREATE TABLE AS`) is allowed on this source|if `DROP TABLE` is allowed on this source|no
space|only in the user's home space, and by manually uploading files in the UI|only in the UI|yes
distributed shared storage (`$scratch` source)|yes|yes|no

As you can see, using the SQL-DDL interface, the location type implies the relation type, so materialization implementations do not have to take care of possible relation type mutations.

The UI allows dots in a space's name : **the adapter does not handle that correctly**.

# Schemas
In dremio, schemas are recursive, like filesystem folders : `dbt.internal."my very strange folder's name"`, and dots are not allowed in sub-folder's names. For each database, there is a root schema, known as `no_schema`by the adapter. So, in order to materialize a model at the root folder of the `track17`space, one will configure it as :

    +database: track17
    +schema: no_schema

**Please note that because dremio has no `CREATE SCHEMA` command yet, all schemas must be created before in the UI or via the API.**
It may change when I replace ODBC with API calls.

# Rendering of a relation

Because dremio accepts almost any string character in the objects' names, the adapter will double quote each part of the database.schema.identifier tryptic with the following rules concerning schema :

 - if schema is equal to `no_schema`, the schema will not be included, leading to a simple `"database"."identifier"` being rendered
 - if schema spans multiple folders, each folder's name will be double quoted, leading to `"database"."folder"."sub-folder"."sub-sub-folder"."identifier"`.

# Sources

In dbt, a source is a set of read-only datasets, foundation of the downstream transformation steps toward the datasets that will be exposed to the end users.

## Environments

A same dremio installation could handle several data environments. In order to group sources by environment, you can use the undocumented `target.profile_name` or the adapter specific `environment` configuration to map environments between dremio and dbt :

 - dremio's side: prefix all the sources' names of a specific environment `prd` with the environment's name, for example : `prd_crm, prd_hr, prd_accounting`
 - dbt's side: prefix all source's database configs like this : `{{target.environment}}_crm` or `{{target.profile_name}}_crm`

That way you can configure seperately input sources and output `databases/datalakes`.

# Materializations

In dbt, a transformation step is called a **model**; defined by a `SELECT` statement embedded in a jinja2 template. Its `FROM` clause may reference source tables and/or other upstream models. A model is also the dataset resulting from this transformation, in fact the kind of SQL object it will be materialized in. Will it be a Common Table Expression used in downstream models ? A view ? A table ? Don't worry, just change the `materialized` parameter's value, and dbt will do that for you !

## Dremio's SQL specificities

Tables and views cannot coexist in a same database/datalake. So the usual dbt database+schema configuration stands only for views. Seeds, tables, incrementals and even stored tests will use a parallel datalake+root_path configuration. This configuration was also added in the profiles.

## Seed

A seed can be viewed as a kind of static model; defined by a csv file, this is also a kind of version controled source table.

adapter's specific configuration|type|required|default
-|-|-|-
datalake|CTAS/DROP TABLE allowed source's name|no|`$scratch`
root_path|the relative path in the datalake|no|`no_schema`
file|don't name the table like the model, use that alias instead|no|

    CREATE TABLE AS
    SELECT *
    FROM VALUES()[,()]

As dremio odbc bridge does not support query bindings (but Arrow flight SQL does...), the python value is converted as string, quoted and casted in the column sql type.

## View

adapter's specific configuration|type|required|default
-|-|-|-
database|any space (or home space) root|no|`@user`
schema|relative path in this space|no|`no_schema`
alias|don't name the view like the model, use that alias instead|no|
    CREATE OR REPLACE VIEW AS
    {{ sql }}

## Table

adapter's specific configuration|type|required|default
-|-|-|-
datalake|CTAS/DROP TABLE allowed source's name|no|`$scratch`
root_path||no|`no_schema`
file|don't name the table like the model, use that alias instead|no|


	 CREATE TABLE tblname [ (field1, field2, ...) ]
	 [ (STRIPED, HASH, ROUNDROBIN) PARTITION BY (field1, field2, ..) ]
     [ DISTRIBUTE BY (field1, field2, ..) ]
     [ LOCALSORT BY (field1, field2, ..) ]
     [ STORE AS (opt1 => val1, opt2 => val3, ...) ]
     [ WITH SINGLE WRITER ]
     [ AS select_statement ]
     
## Incremental

This is a very interesting materialization. An incremental transformation does not only reference other models and/or sources, but also itself.
As the `SELECT` statement is embedded in a jinja2 template, it can be written so to produce two distinct datasets using the `is_incremental()` macro; one for (re)initialization; one for incremental update, based on the current content of the already created dataset. The SQL will reference the current dataset state with the special `{{ this }}` relation.

### the `append`strategy is available in dbt when `dremio.iceberg.ctas.enabled=yes` in dremio.

adapter's specific configuration|type|required|default
-|-|-|-
datalake|CTAS/DROP TABLE allowed source's name|no|`$scratch`
root_path||no|`no_schema`
incremental_strategy| only `append` for the moment|no|`append`
on_schema_change| `sync_all_columns`, `append_new_columns`, `fail`, `ignore`|no|`ignore`
file|don't name the table like the model, use that alias instead|no|

Other strategies will be implemented when dremio can `INSERT OVERWRITE` or `MERGE/UPDATE` in an iceberg table.

## Reflection

A reflection is a materialization of a dataset (its anchor), with a refresh policy, handled internally by dremio, of three different kinds : 
- a **raw** reflection will act as a materialized view of all or a subset of an upstream model's columns (usually a view)
- a **aggregate** reflection is much like a mondrian aggregation table, pre-aggregated measures on a subset of dimension columns
- a **external** reflection just tell dremio to use a dataset (external target) as a possible materialization of another dataset.

The `dremio:reflections_enabled` boolean dbt variable can be used to disable reflection management in dbt. 
That way, you can still use dbt ontop dremio enterprise edition, even without admin rights needed to read `sys.reflections` table.

adapter's specific configuration|reflection type|type|required|default
-|-|-|-|-
anchor|all but external|the anchor model name|only if there is more than one `-- depends_on` clause in the model SQL|
reflection_type|all|`raw`, `aggregate` or `external`|no|`raw`
external_target|external| the underlying target|yes|
display|raw|list of columns|no|all columns
dimensions|aggregate|list of dimension columns|no|all non decimal/float/double columns
dimensions_by_day|aggregate|list of dimension timestamp columns we want to keep only the date part of|no|all timestamp columns
measures|aggregate|list of measure columns|no|all decimal/float/double columns
computations|aggregate|list of specific [computations](https://docs.dremio.com/software/sql-reference/sql-commands/acceleration/#aggregate-reflections)|no|`SUM, COUNT` for each measure (in array)
arrow_cache|all but external|is the reflection using arrow caching ?|no|`false`
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

The model definition will not contain a `SELECT` statement, but a simple : 

	 -- depends_on: {{ ref('my_anchor') }}

## Format configuration

For persisted models, a format can be specified in its `config` block; for a source table, in its `external` properties block.

Seed, table and incremental materializations share the same format configuration :

in `config` or `external`blocks|format|type|required|default
-|-|-|-|-
format||`text`, `json`, `arrow`, `parquet`, `iceberg`|no|`iceberg`
field_delimiter|text|field delimiter character|no|
line_delimiter|text|line delimiter character|no|
quote|text|quote character|no|
comment|text|comment character|no|
escape|text|escape character|no|
skip_first_line|text|do not read first line ?|no|
extract_header|text|extract header ?|no|
trim_header|text|trim header column names ?|no|
auto_generated_column_names|text|auto generate column names ?|no|
pretty_print|json|write human readable json ?|no

It's all the same for sources, with a few extra configurations : 
in `external`block |format|type|required|default
-|-|-|-|-
format||`excel`, `delta` (deltalake)|no|
extract_header|text, excel|extract header from first line ?|no|
sheet_name|excel|sheet's name in the excel file|no|
xls|excel|is it an old excel file, not a xlsx one ?|no|
has_merged_cells|excel|are there any merged cells ?|no|

## Partitioning configuration

Any materialization except `view` can be partitioned. Dremio will add as many `dir0, dir1...` columns as needed to let the partitioning scheme show up in the source table, or model.

`config`|materialization|type|required|default
-|-|-|-|-
partition_method|all but reflection|`striped`, `hash`, `roundrobin`|no|
partition_method|reflection|`striped`, `consolidated`|no|
partition_by|all |partition columns|no|
localsort_by|all |sort columns within partition|no|
distribute_by|all |distribution columns|no|
single_writer|all but reflection|disable parallel write, incompatible with partition_by|no|

## Twin strategy configuration

As tables and views cannot coexist neither in spaces or datalakes, when a model changes relation type, from view to incremental materialization for example, we can end up with both a view in a space, and a table in a datalake. 

At model level, dbt can apply a 'twin' strategy :
 - **allow** sql object homonyms of different types (relaxed behavior) : if a model changes relation type, the previous table or view remains.
 - **prevent** sql object homonym creation, dropping the previous relation of different type if it exists : the previous table or view is dropped.
 - **clone** a table relation as a view, in order to have a direct access to the model's dataset from the space layer. That time the view is neither left untouched nor dropped, but its definition is replaced with a straight `select * from {{ the_new_table_relation }}`.

`config`|materialization|type|required|default
-|-|-|-|-
twin_strategy|every materialization but reflection|`allow`, `prevent`, `clone`|no|`clone`

It should be safe as long as you don't play with `alias` and/or `file` configs.

## External queries

dremio can send native SQL to external sources, this is known as ["External Queries"](https://docs.dremio.com/software/data-sources/external-queries/).

The SQL must not contain any `ref()` or `source()`, only `[schema.]table` paths. 

At least one `-- depends_on : {{ source(my_source, my_table) }}` must be added to the model's SQL instead. To let source table dependencies show up in the auto generated documentation, you should indeed mention all the source tables used in the external query.

`config`|materialization|type|required|default
-|-|-|-|-
external_query|view, table or incremental|`true` or `false`|no|`false`

dbt will render the first source table like this : `{{ source(my_source, my_table).include(schema=false, identifier=false }}` to obtain the final source database and build the external query call :

	select *
	from table("source_name".external_query('sql'))

# Connection

Be careful to provide the right odbc driver's name in the adapter specific `driver` attribute, the one you gave to your dremio's odbc driver installation.

Here are the profile default values :

configuration | default
-|-
database|@user
schema|no_schema
datalake|$scratch
root_path|no_schema

## Dremio Software

With this default configuration, one can start trying dbt on dremio out of the box, as any dremio may have a user home space and a $scratch file system.

    track17:
      outputs:
        unmanaged:
          type: dremio
          threads: 2
          driver: Dremio ODBC Driver 64-bit
          host: veniseverte.fr
          port: 31010
          user: fabrice_etanchaud
          password: fabricesecretpassword
        managed:
          type: dremio
          threads: 2
          driver: Dremio ODBC Driver 64-bit
          host: veniseprovencale.fr
          port: 31010
          environment: track17
          database: '@dremio'
          schema: no_schema
          datalake: my_s3
          root_path: part.comp.biz
          user: dremio
          password: dremiosecretpassword
      target: unmanaged

## Dremio Cloud

For Dremio Cloud, you will have to make an additional configuration change to the `dbt_project.yml`.
Add the following to `dbt_project.yml`:

    vars:
      dremio:reflections_enabled: false

The profile will look similar to below:

    [project_name]:
      outputs:
        test:
          type: dremio
          threads: 2
          driver: Dremio ODBC Driver 64-bit
          host: sql.dremio.cloud
          port: 443
          database: '@[YOUR_EMAIL]'
          schema: no_schema
          user: $token
          password: [PAT]
          additional_parameters: SSL=1;
      target: test

# Behind the scenes
## How dremio does "format on read" ?

Dremio has an interesting feature : it can format a raw dataset "on read" that way : 

    select * 
    from table(
	    "datalake"."root_path1"."root_path2"."identifier" 
	    (type=>'text', fieldDelimiter=>';')
	   )
This adapter uses that feature to render a decorated `Relation` of a formatted model or source table : instead of the usual `"datalake"."root_path1"."root_path2"."identifier"`,  the `ref()` and `source()` macros are overridden to read the format from the node's `model.config` or `source.external` block, and decorate the path given by their `builtins` version.

This has a drawback : A formatted source table or a formatted model cannot be a reflection's anchor. You will have to create a proxy view.

## How dbt-dremio handle custom `datalake`/`root_path`

Final`database` and `schema` model configurations are a mix of their target and custom values. The rules are defined in the well known `get_custom_(database|schema)_name` macros. 

`datalake` and `root_path` model configurations were introduced to circumvent the segregation dremio imposes between views and tables, and fit the target/custom handling. These macros were adapted to this end.
If needed, please override the `get_custom_(database|schema)_name_impl` macros instead, to  keep everything wired.
