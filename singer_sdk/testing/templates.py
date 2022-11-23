"""Tap and Target Test Templates."""

from __future__ import annotations

import contextlib
import os
import warnings
from pathlib import Path
from typing import Any, List, Self, Type, Union

from singer_sdk.streams import Stream

from .runners import TapTestRunner, TargetTestRunner


class TestTemplate:
    """Each Test class requires one or more of the following arguments.

    Args:
        runner (SingerTestRunner): The singer runner for this test.

    Possible Args:
        stream (obj, optional): Initialized stream object to be tested.
        stream_name (str, optional): Name of the stream to be tested.
        stream_records (list[obj]): Array of records output by the stream sync.
        attribute_name (str, optional): Name of the attribute to be tested.

    Raises:
        ValueError: [description]
        NotImplementedError: [description]
        NotImplementedError: [description]
    """

    name: str | None = None
    type: str | None = None

    @property
    def id(self) -> str:
        """Test ID.

        Raises:
            NotImplementedError: if not implemented.
        """
        raise NotImplementedError("ID not implemented.")

    def setup(self) -> None:
        """Test setup, called before `.test()`.

        This method is useful for preparing external resources (databases, folders etc.)
        before test execution.

        Raises:
            NotImplementedError: if not implemented.
        """
        raise NotImplementedError("Setup method not implemented.")

    def test(self) -> None:
        """Main Test body, called after `.setup()` and before `.validate()`."""
        self.runner.sync_all()

    def validate(self) -> None:
        """Test validation, called after `.test()`.

        This method is particularly useful in Target tests, to validate that records
        were correctly written to external systems.

        Raises:
            NotImplementedError: if not implemented.
        """
        raise NotImplementedError("Method not implemented.")

    def teardown(self) -> None:
        """Test Teardown.

        This method is useful for cleaning up external resources (databases, folders etc.)
        after test completion.

        Raises:
            NotImplementedError: if not implemented.
        """
        raise NotImplementedError("Method not implemented.")

    def run(
        self, resource: Any, runner: Union[Type[TapTestRunner], Type[TargetTestRunner]]
    ) -> None:
        """Test main run method.

        Args:
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap or Target runner instance, to use with this test.

        Raises:
            ValueError: if Test instance does not have `name` and `type` properties.
        """
        if not self.name or not self.type:
            raise ValueError("Test must have 'name' and 'type' properties.")

        self.resource = resource
        self.runner = runner

        with contextlib.suppress(NotImplementedError):
            self.setup()

        try:
            self.test()
            with contextlib.suppress(NotImplementedError):
                self.validate()

        finally:
            with contextlib.suppress(NotImplementedError):
                self.teardown()


class TapTestTemplate(TestTemplate):
    """Base Tap test template."""

    type = "tap"

    @property
    def id(self) -> str:
        """Test ID.

        Returs:
            Test ID string.
        """
        return f"tap__{self.name}"

    def run(
        self, resource: Any, runner: Union[Type[TapTestRunner], Type[TargetTestRunner]]
    ) -> None:
        """Test main run method.

        Args:
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap or Target runner instance, to use with this test.

        Raises:
            ValueError: if Test instance does not have `name` and `type` properties.
        """
        self.tap = runner.tap
        super().run(resource, runner)


class StreamTestTemplate(TestTemplate):
    """Base Tap Stream test template."""

    type = "stream"
    required_kwargs = ["stream", "stream_records"]

    @property
    def id(self) -> str:
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"{self.stream.name}__{self.name}"

    def run(
        self,
        resource: Any,
        runner: Type[TapTestRunner],
        stream: Type[Stream],
        stream_records: List[dict],
    ) -> None:
        """Test main run method.

        Args:
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
            stream: A Tap Stream instance, to use with this test.
            stream_records: The records returned by the given Stream,
                to use with this test.
        """
        self.stream = stream
        self.stream_records = stream_records
        super().run(resource, runner)


class AttributeTestTemplate(TestTemplate):
    """Base Tap Stream Attribute template."""

    type = "attribute"

    @property
    def id(self) -> str:
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"{self.stream.name}__{self.attribute_name}__{self.name}"

    def run(
        self,
        resource: Any,
        runner: Type[TapTestRunner],
        stream: Type[Stream],
        stream_records: List[dict],
        attribute_name: str,
    ) -> None:
        """Test main run method.

        Args:
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
            stream: A Tap Stream instance, to use with this test.
            stream_records: The records returned by the given Stream,
                to use with this test.
            attribute_name: The name of the attribute to test.
        """
        self.stream = stream
        self.stream_records = stream_records
        self.attribute_name = attribute_name
        super().run(resource, runner)

    @property
    def non_null_attribute_values(self) -> List[Any]:
        """Extract attribute values from stream records.

        Returns:
            A list of attribute values (excluding None values).
        """
        values = [
            r[self.attribute_name]
            for r in self.stream_records
            if r.get(self.attribute_name) is not None
        ]
        if not values:
            warnings.warn(UserWarning("No records were available to test."))
        return values

    @classmethod
    def evaluate(
        cls: Type[Self @ AttributeTestTemplate],
        stream: Type[Stream],
        property_name: str,
        property_schema: dict,
    ) -> bool:
        """Determine if this attribute test is applicable to the given property.

        Args:
            stream: Parent Stream of given attribute.
            property_name: Name of given attribute.
            property_schema: JSON Schema of given property, in dict form.

        Raises:
            NotImplementedError: if not implemented.
        """
        raise NotImplementedError(
            "The 'evaluate' method is required for attribute tests, but not implemented."
        )


class TargetTestTemplate(TestTemplate):
    """Base Target test template."""

    type = "target"

    def run(self, resource: Any, runner: Type[TapTestRunner]) -> None:
        """Test main run method.

        Args:
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
        """
        self.target = runner.target
        super().run(resource, runner)

    @property
    def id(self):
        """Test ID.

        Returns:
            Test ID string.
        """
        return f"target__{self.name}"


class TargetFileTestTemplate(TargetTestTemplate):
    """Base Target File Test Template.

    Use this when sourcing Target test input from a .singer file.
    """

    def run(self, resource: Any, runner: Type[TapTestRunner]):
        """Test main run method.

        Args:
            resource: A generic external resource, provided by a pytest fixture.
            runner: A Tap runner instance, to use with this test.
        """
        # get input from file
        if getattr(self, "singer_filepath", None):
            assert Path(
                self.singer_filepath
            ).exists(), f"Singer file {self.singer_filepath} does not exist."
            runner.input_filepath = self.singer_filepath
        super().run(resource, runner)

    @property
    def singer_filepath(self) -> Path:
        """Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns:
            The expected Path to this tests singer file.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_singer_filepath = os.path.join(current_dir, "target_test_streams")
        return os.path.join(base_singer_filepath, f"{self.name}.singer")
