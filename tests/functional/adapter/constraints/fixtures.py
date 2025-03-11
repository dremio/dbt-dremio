dremio_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar
      - name: date_day
        data_type: varchar
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar
      - name: date_day
        data_type: varchar
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar
      - name: date_day
        data_type: varchar
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar
      - name: date_day
        data_type: varchar
"""

dremio_constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: primary_key
        columns: [ id ]
      - type: unique
        columns: [ color, date_day ]
        name: strange_uniqueness_requirement
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: color
        data_type: varchar
      - name: date_day
        data_type: varchar
"""

dremio_model_quoted_column_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
      materialized: table
    constraints:
      - type: check
        # this one is the on the user
        expression: ("from" = 'blue')
        columns: [ '"from"' ]
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: from  # reserved word
        quote: true
        data_type: varchar
        constraints:
          - type: not_null
      - name: date_day
        data_type: varchar
"""