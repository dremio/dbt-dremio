from operator import contains
from dbt.tests.util import check_relations_equal_with_relations
from typing import List
from contextlib import contextmanager

from dbt.events import AdapterLogger

logger = AdapterLogger("dremio")


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

    # Make sure we have database/schema/identifier parts, even if
    # only identifier was supplied.
    relation_parts = name.split(".")
    if len(relation_parts) == 1:
        relation_parts.insert(0, credentials.schema)
    if len(relation_parts) == 2:
        # if the relation is a view then use database
        if materialization == "view" or "view" in name:
            relation_parts.insert(0, credentials.database)
        else:
            relation_parts.insert(0, credentials.datalake)
    kwargs = {
        "database": relation_parts[0],
        "schema": relation_parts[1],
        "identifier": relation_parts[2],
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


def check_relation_types(adapter, relation_to_type):
    # Ensure that models with different materialiations have the
    # corrent table/view.
    # Uses:
    #   adapter.list_relations_without_caching
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
                assert relation.type == value, (
                    f"Got an unexpected relation type of {relation.type} "
                    f"for relation {key}, expected {value}"
                )


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
