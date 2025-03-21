# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dbt.tests.util import check_relations_equal_with_relations
from typing import List
from contextlib import contextmanager
from dbt.tests.util import AnyInteger

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")

# Ensure we do not include dashes in our source
# https://github.com/dremio/dbt-dremio/issues/68
BUCKET = "dbtdremios3"
SOURCE = "dbt_test_source"


class TestProcessingException(Exception):
    pass


def relation_from_name(adapter, name: str, materialization=""):
    """reverse-engineer a relation from a given name and
    the adapter. The relation name is split by the '.' character.
    """

    # Different adapters have different Relation classes
    cls = adapter.Relation
    credentials = adapter.config.credentials
    quote_policy = cls.get_default_quote_policy().to_dict()
    include_policy = cls.get_default_include_policy().to_dict()

    relation_parts = name.split(".")

    # Make sure we have database/schema/identifier parts, even if
    # only identifier was supplied.

    # if the relation is a view then use database
    if materialization == "view" or (materialization != "table" and "view" in name.lower()):
        relation_parts.insert(0, credentials.database)
        relation_parts.insert(1, credentials.schema)
    else:
        relation_parts.insert(0, credentials.datalake)
        if credentials.root_path not in [None, "no_schema"]:
            relation_parts.insert(1, credentials.root_path)
    

    relation_type = "table" if materialization != "view" and "view" not in name else "view"

    schema = ".".join(relation_parts[1:-1])

    kwargs = {
        "database": relation_parts[0],
        "schema": None if schema == "" else schema, 
        "identifier": relation_parts[-1],
        "type": relation_type,
    }

    relation = cls.create(
        include_policy=include_policy,
        quote_policy=quote_policy,
        **kwargs,
    )
    return relation


@contextmanager
def get_connection(adapter, name="_test"):
    with adapter.connection_named(name):
        conn = adapter.connections.get_thread_connection()
        yield conn

# Overwrite the default implementation to use this adapter's
# relation_from_name function. 
def get_relation_columns(adapter, name):
    relation = relation_from_name(adapter, name)
    with get_connection(adapter):
        columns = adapter.get_columns_in_relation(relation)
        return sorted(((c.name, c.dtype, c.char_size) for c in columns), key=lambda x: x[0])


# Overwrite the default implementation to use this adapter's
# relation_from_name function. 
def check_relation_types(adapter, relation_to_type):
    # Ensure that models with different materialiations have the
    # corrent table/view.
    """
    Relation name to table/view
    {
        "base": "table",
        "other": "view",
    }
    """

    expected_relation_values = {}
    found_relations = []
    schemas = set()

    for key, value in relation_to_type.items():
        relation = relation_from_name(adapter, key, value)
        expected_relation_values[relation] = value
        schemas.add(relation.without_identifier())

        with get_connection(adapter):
            for schema in schemas:
                found_relations.extend(adapter.list_relations_without_caching(schema))

    for key, value in relation_to_type.items():
        for relation in found_relations:
            # this might be too broad
            if relation.identifier == key:
                assert relation.type == value, (  # nosec assert_used
                    f"Got an unexpected relation type of {relation.type} "
                    f"for relation {key}, expected {value}"
                )


# Overwrite the default implementation to use this adapter's
# relation_from_name function. 
def check_relations_equal(adapter, relation_names: List, compare_snapshot_cols=False):
    # Replaces assertTablesEqual. assertManyTablesEqual can be replaced
    # by doing a separate call for each set of tables/relations.
    # Wraps check_relations_equal_with_relations by creating relations
    # from the list of names passed in.
    if len(relation_names) < 2:
        raise TestProcessingException(
            "Not enough relations to compare",
        )
    relations = [relation_from_name(adapter, name) for name in relation_names]
    return check_relations_equal_with_relations(
        adapter, relations, compare_snapshot_cols=compare_snapshot_cols
    )


def base_expected_catalog(
    project,
    role,
    id_type,
    text_type,
    time_type,
    view_type,
    table_type,
    model_stats,
    seed_stats=None,
    case=None,
    case_columns=False,
):

    if case is None:

        def case(x):
            return x

    col_case = case if case_columns else lambda x: x

    if seed_stats is None:
        seed_stats = model_stats

    model_database = project.database
    my_schema_name = case(project.test_schema)
    alternate_schema = case(project.test_schema + ".test")

    expected_cols = {
        col_case("id"): {
            "name": col_case("id"),
            "index": AnyInteger(),
            "type": id_type,
            "comment": None,
        },
        col_case("first_name"): {
            "name": col_case("first_name"),
            "index": AnyInteger(),
            "type": text_type,
            "comment": None,
        },
        col_case("email"): {
            "name": col_case("email"),
            "index": AnyInteger(),
            "type": text_type,
            "comment": None,
        },
        col_case("ip_address"): {
            "name": col_case("ip_address"),
            "index": AnyInteger(),
            "type": text_type,
            "comment": None,
        },
        col_case("updated_at"): {
            "name": col_case("updated_at"),
            "index": AnyInteger(),
            "type": time_type,
            "comment": None,
        },
    }
    return {
        "nodes": {
            "model.test.model": {
                "unique_id": "model.test.model",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("model"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": expected_cols,
            },
            "model.test.second_model": {
                "unique_id": "model.test.second_model",
                "metadata": {
                    "schema": alternate_schema,
                    "database": project.database,
                    "name": case("second_model"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": expected_cols,
            },
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name, 
                    "database": SOURCE,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": expected_cols,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": SOURCE,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": expected_cols,
            },
        },
    }


def expected_references_catalog(
    project,
    role,
    id_type,
    text_type,
    time_type,
    view_type,
    table_type,
    model_stats,
    bigint_type=None,
    seed_stats=None,
    case=None,
    case_columns=False,
    view_summary_stats=None,
):
    if case is None:

        def case(x):
            return x

    col_case = case if case_columns else lambda x: x

    if seed_stats is None:
        seed_stats = model_stats

    if view_summary_stats is None:
        view_summary_stats = model_stats

    model_database = project.database
    my_schema_name = case(project.test_schema)

    summary_columns = {
        "first_name": {
            "name": "first_name",
            "index": 1,
            "type": text_type,
            "comment": None,
        },
        "ct": {
            "name": "ct",
            "index": 2,
            "type": bigint_type,
            "comment": None,
        },
    }

    seed_columns = {
        "id": {
            "name": col_case("id"),
            "index": 1,
            "type": id_type,
            "comment": None,
        },
        "first_name": {
            "name": col_case("first_name"),
            "index": 2,
            "type": text_type,
            "comment": None,
        },
        "email": {
            "name": col_case("email"),
            "index": 3,
            "type": text_type,
            "comment": None,
        },
        "ip_address": {
            "name": col_case("ip_address"),
            "index": 4,
            "type": text_type,
            "comment": None,
        },
        "updated_at": {
            "name": col_case("updated_at"),
            "index": 5,
            "type": time_type,
            "comment": None,
        },
    }
    return {
        "nodes": {
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name,
                    "database": SOURCE,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": seed_columns,
            },
            "model.test.ephemeral_summary": {
                "unique_id": "model.test.ephemeral_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": SOURCE,
                    "name": case("ephemeral_summary"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": summary_columns,
            },
            "model.test.view_summary": {
                "unique_id": "model.test.view_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("view_summary"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": view_summary_stats,
                "columns": summary_columns,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("seed"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": seed_columns,
            },
        },
    }
