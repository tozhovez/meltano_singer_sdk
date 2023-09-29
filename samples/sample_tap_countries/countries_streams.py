"""Sample tap stream test for tap-countries.

This uses a free "Countries API" which does not require authentication.

See the online explorer and query builder here:
  - https://countries.trevorblades.com/
"""

from __future__ import annotations

import abc
import sys
import typing as t

from graphql_query import Field, Operation, Query

from singer_sdk import typing as th
from singer_sdk.helpers._catalog import FieldTree, selection_to_tree
from singer_sdk.helpers._compat import importlib_resources
from singer_sdk.streams.graphql import GraphQLStream

if t.TYPE_CHECKING:
    from singer_sdk._singerlib.catalog import SelectionMask

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


def _tree_to_fields(tree: FieldTree) -> list[Field]:
    """Convert a tree to a list of GraphQL fields."""
    return [
        Field(name=name, fields=_tree_to_fields(subtree))
        for name, subtree in tree.items()
    ]


def selection_to_fields(selection: SelectionMask) -> list[Field]:
    """Convert a selection mask to a GraphQL query."""
    tree = selection_to_tree(selection)
    return _tree_to_fields(tree)


class CountriesAPIStream(GraphQLStream, metaclass=abc.ABCMeta):
    """Sample tap test for countries.

    NOTE: This API does not require authentication.
    """

    url_base = "https://countries.trevorblades.com/"


class CountriesStream(CountriesAPIStream):
    """Countries API stream."""

    name = "countries"
    primary_keys = ("code",)
    # query = """
    #     countries {
    #         code
    #         name
    #         native
    #         phone
    #         continent {
    #             code
    #             name
    #         }
    #         capital
    #         currency
    #         languages {
    #             code
    #             name
    #         }
    #         emoji
    #     }
    #     """
    schema = th.PropertiesList(
        th.Property("code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("native", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("capital", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("emoji", th.StringType),
        th.Property(
            "continent",
            th.ObjectType(
                th.Property("code", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "languages",
            th.ArrayType(
                th.ObjectType(
                    th.Property("code", th.StringType),
                    th.Property("name", th.StringType),
                ),
            ),
        ),
    ).to_dict()

    @property
    def query(self) -> str:
        """Return the GraphQL query string."""
        countries = Query(
            name="countries",
            fields=selection_to_fields(self.mask),
            # fields=[
            #     Field(name="code"),
            #     Field(name="name"),
            #     Field(name="native"),
            #     Field(name="phone"),
            #     Field(name="capital"),
            #     Field(name="currency"),
            #     Field(name="emoji"),
            #     Field(
            #         name="continent",
            #         fields=[
            #             Field(name="code"),
            #             Field(name="name"),
            #         ],
            #     ),
            #     Field(
            #         name="languages",
            #         fields=[
            #             # Field(name="code"),
            #             # Field(name="name"),
            #         ],
            #     ),
            # ],
        )
        print(countries, file=sys.stderr)
        return Operation(
            type="query",
            queries=[
                countries,
            ],
        ).render()


class ContinentsStream(CountriesAPIStream):
    """Continents stream from the Countries API."""

    name = "continents"
    primary_keys = ("code",)
    schema_filepath = SCHEMAS_DIR / "continents.json"
    query = """
        continents {
            code
            name
        }
        """
