"""Abstract base classes for all Singer messages IO operations."""

from __future__ import annotations

import abc
import decimal
import json
import logging
import os
import sys
import typing as t
from collections import Counter, defaultdict

from singer_sdk import metrics
from singer_sdk._singerlib.messages import Message, SingerMessageType
from singer_sdk._singerlib.messages import format_message as singer_format_message
from singer_sdk._singerlib.messages import write_message as singer_write_message
from singer_sdk.exceptions import InvalidInputLine

logger = logging.getLogger(__name__)


class SingerReader(metaclass=abc.ABCMeta):
    """Interface for all plugins reading Singer messages from stdin."""

    @t.final
    def listen(self, file_input: t.IO[str] | None = None) -> None:
        """Read from input until all messages are processed.

        Args:
            file_input: Readable stream of messages. Defaults to standard in.

        This method is internal to the SDK and should not need to be overridden.
        """
        if not file_input:
            file_input = sys.stdin

        load_timer = metrics.Timer(
            metrics.Metric.SYNC_DURATION,
            {"pid": os.getpid()},
        )

        with load_timer:
            self._process_lines(file_input)
            self._process_endofpipe()

    @staticmethod
    def _assert_line_requires(line_dict: dict, requires: set[str]) -> None:
        """Check if dictionary .

        Args:
            line_dict: TODO
            requires: TODO

        Raises:
            InvalidInputLine: raised if any required keys are missing
        """
        if not requires.issubset(line_dict):
            missing = requires - set(line_dict)
            msg = f"Line is missing required {', '.join(missing)} key(s): {line_dict}"
            raise InvalidInputLine(msg)

    def deserialize_json(self, line: str) -> dict:  # noqa: PLR6301
        """Deserialize a line of json.

        Args:
            line: A single line of json.

        Returns:
            A dictionary of the deserialized json.

        Raises:
            json.decoder.JSONDecodeError: raised if any lines are not valid json
        """
        try:
            return json.loads(  # type: ignore[no-any-return]
                line,
                parse_float=decimal.Decimal,
            )
        except json.decoder.JSONDecodeError as exc:
            logger.exception("Unable to parse:\n%s", line, exc_info=exc)
            raise

    def _process_lines(self, file_input: t.IO[str]) -> t.Counter[str]:
        """Internal method to process jsonl lines from a Singer tap.

        Args:
            file_input: Readable stream of messages, each on a separate line.

        Returns:
            A counter object for the processed lines.
        """
        stats: dict[str, int] = defaultdict(int)
        record_message_counter = metrics.Counter(
            metrics.Metric.MESSAGE_COUNT,
            {"pid": os.getpid()},
            log_interval=metrics.DEFAULT_LOG_INTERVAL,
        )
        with record_message_counter:
            for line in file_input:
                line_dict = self.deserialize_json(line)
                self._assert_line_requires(line_dict, requires={"type"})

                record_type: SingerMessageType = line_dict["type"]
                if record_type == SingerMessageType.SCHEMA:
                    self._process_schema_message(line_dict)

                elif record_type == SingerMessageType.RECORD:
                    record_message_counter.increment()
                    self._process_record_message(line_dict)

                elif record_type == SingerMessageType.ACTIVATE_VERSION:
                    self._process_activate_version_message(line_dict)

                elif record_type == SingerMessageType.STATE:
                    self._process_state_message(line_dict)

                elif record_type == SingerMessageType.BATCH:
                    self._process_batch_message(line_dict)

                else:
                    self._process_unknown_message(line_dict)  # pragma: no cover

                stats[record_type] += 1

        return Counter(**stats)

    @abc.abstractmethod
    def _process_schema_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_record_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_state_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_activate_version_message(self, message_dict: dict) -> None: ...

    @abc.abstractmethod
    def _process_batch_message(self, message_dict: dict) -> None: ...

    def _process_unknown_message(self, message_dict: dict) -> None:  # noqa: PLR6301
        """Internal method to process unknown message types from a Singer tap.

        Args:
            message_dict: Dictionary representation of the Singer message.

        Raises:
            ValueError: raised if a message type is not recognized
        """
        record_type = message_dict["type"]
        msg = f"Unknown message type '{record_type}' in message."
        raise ValueError(msg)

    def _process_endofpipe(self) -> None:  # noqa: PLR6301
        logger.debug("End of pipe reached")


class SingerWriter:
    """Interface for all plugins writting Singer messages to stdout."""

    def format_message(self, message: Message) -> str:  # noqa: PLR6301
        """Format a message as a JSON string.

        Args:
            message: The message to format.

        Returns:
            The formatted message.
        """
        return singer_format_message(message)

    def write_message(self, message: Message) -> None:  # noqa: PLR6301
        """Write a message to stdout.

        Args:
            message: The message to write.
        """
        singer_write_message(message)
