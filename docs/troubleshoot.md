# Troubleshooting Guide

## Error: `non-view table with given name`

This error refers to creating a table and view being created in the same namespace. This can be resolved by setting the `twin-strategy` value on the model, this setting is [documented here](https://github.com/dremio/dbt-dremio/wiki/Using-Materializations-with-Dremio#optional-twin-strategy-configuration).

Here is an example allowing a table/view with the same name using `config` in an individual model:

```python
{{ config(
    materialized="table"
    table_type="iceberg"
    twin_strategy="allow"
)
}}
```

## Error: ``