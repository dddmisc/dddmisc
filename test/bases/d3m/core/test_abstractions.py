import datetime as dt
import uuid
from functools import cached_property

import pytest

from d3m.core import (
    IMessage,
    MessageType,
    MessageName,
    DomainName,
    AbstractCommand,
    AbstractEvent,
    UniversalMessage,
)


@pytest.fixture
def fake_command_class():
    class FakeCommand(IMessage):
        @property
        def __domain_name__(self) -> DomainName:
            return DomainName("test")

        @property
        def __message_name__(self) -> MessageName:
            return MessageName("Test")

        @property
        def __type__(self) -> MessageType:
            return MessageType.COMMAND

        @cached_property
        def __reference__(self) -> uuid.UUID:
            return uuid.uuid4()

        @cached_property
        def __timestamp__(self) -> dt.datetime:
            return dt.datetime.now(dt.UTC)

        @property
        def __payload__(self) -> dict:
            return {"foo": "bar"}

        def to_dict(self) -> dict:
            return {}

        def to_json(self) -> str:
            return "{}"

    return FakeCommand


@pytest.fixture
def fake_event_class():
    class FakeEvent(IMessage):
        @property
        def __domain_name__(self) -> DomainName:
            return DomainName("test")

        @property
        def __message_name__(self) -> MessageName:
            return MessageName("Test")

        @property
        def __type__(self) -> MessageType:
            return MessageType.EVENT

        @cached_property
        def __reference__(self) -> uuid.UUID:
            return uuid.uuid4()

        @cached_property
        def __timestamp__(self) -> dt.datetime:
            return dt.datetime.now(dt.UTC)

        @property
        def __payload__(self) -> dict:
            pass

        def to_dict(self) -> dict:
            pass

        def to_json(self) -> str:
            pass

    return FakeEvent


class TestMessageAbstractions:
    def test_command_instance(self, fake_command_class):
        cmd = fake_command_class()
        assert isinstance(cmd, IMessage)
        assert isinstance(cmd, AbstractCommand)

    def test_command_equal(self, fake_command_class):
        cmd = fake_command_class()
        msg = UniversalMessage(
            "test.Test",
            MessageType.COMMAND,
            {},
            reference=cmd.__reference__,
        )
        assert cmd == msg
        assert hash(cmd) == hash(msg)

        msg = UniversalMessage(
            "test.Test",
            MessageType.COMMAND,
            {},
        )

        assert cmd != msg
        assert hash(cmd) != hash(msg)

    def test_event_instance(self, fake_event_class):
        cmd = fake_event_class()
        assert isinstance(cmd, IMessage)
        assert isinstance(cmd, AbstractEvent)

    def test_event_equal(self, fake_event_class):
        event = fake_event_class()
        msg = UniversalMessage(
            "test.Test",
            MessageType.EVENT,
            {},
            reference=event.__reference__,
        )
        assert event == msg
        assert hash(event) == hash(msg)

        msg = UniversalMessage(
            "test.Test",
            MessageType.EVENT,
            {},
        )

        assert event != msg
        assert hash(event) != hash(msg)
