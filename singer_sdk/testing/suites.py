"""Standard Tap and Target test suites."""

from __future__ import annotations

from dataclasses import dataclass

from .tap_tests import (
    AttributeIsBooleanTest,
    AttributeIsDateTimeTest,
    AttributeIsIntegerTest,
    AttributeIsNumberTest,
    AttributeIsObjectTest,
    AttributeNotNullTest,
    StreamCatalogSchemaMatchesRecordTest,
    StreamPrimaryKeysTest,
    StreamRecordSchemaMatchesCatalogTest,
    StreamReturnsRecordTest,
    TapCLIPrintsTest,
    TapDiscoveryTest,
    TapStreamConnectionTest,
)

# TODO: add TargetMultipleStateMessages
# TODO: fix behavior in SDK to make this pass
from .target_tests import (
    TargetArrayData,
    TargetCamelcaseComplexSchema,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetEncodedStringData,
    TargetInvalidSchemaTest,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)
from .templates import TapTestTemplate, TargetTestTemplate, TestTemplate


@dataclass
class SuiteConfig:
    """Test Suite Config, passed to each test.

    Args:
        max_records_limit: Max records to fetch during tap testing.
        ignore_no_records: Ignore stream test failures if stream returns no records,
            for all streams.
        ignore_no_records_for_streams: Ignore stream test failures if stream returns
             no records, for named streams.
    """

    max_records_limit: int | None = 5
    ignore_no_records: bool = False
    ignore_no_records_for_streams: list[str] = []


@dataclass
class TestSuite:
    """Test Suite container class."""

    kind: str
    tests: list[type[TestTemplate] | type[TapTestTemplate] | type[TargetTestTemplate]]


# Tap Test Suites
tap_tests = TestSuite(
    kind="tap", tests=[TapCLIPrintsTest, TapDiscoveryTest, TapStreamConnectionTest]
)
tap_stream_tests = TestSuite(
    kind="tap_stream",
    tests=[
        StreamCatalogSchemaMatchesRecordTest,
        StreamRecordSchemaMatchesCatalogTest,
        StreamReturnsRecordTest,
        StreamPrimaryKeysTest,
    ],
)
tap_stream_attribute_tests = TestSuite(
    kind="tap_stream_attribute",
    tests=[
        AttributeIsBooleanTest,
        AttributeIsDateTimeTest,
        AttributeIsIntegerTest,
        AttributeIsNumberTest,
        AttributeIsObjectTest,
        AttributeNotNullTest,
    ],
)


# Target Test Suites
target_tests = TestSuite(
    kind="target",
    tests=[
        TargetArrayData,
        TargetCamelcaseComplexSchema,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetEncodedStringData,
        TargetInvalidSchemaTest,
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
    ],
)
