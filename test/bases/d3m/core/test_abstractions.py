import datetime as dt
from typing import Mapping
from uuid import UUID

from d3m.core import (
    IMessage,
    MessageType,
    MessageName,
    DomainName,
    AbstractCommand,
    AbstractEvent,
    IMessageMeta,
)


class TestMessageAbstractions:
    def test_command_instance(self):
        class FakeCommandMeta(IMessageMeta):
            @property
            def __domain_name__(cls) -> DomainName:
                pass

            @property
            def __message_name__(cls) -> MessageName:
                pass

            @property
            def __type__(cls) -> MessageType:
                return MessageType.COMMAND

            def load(
                cls,
                payload: Mapping | str | bytes,
                reference: UUID | None = None,
                timestamp: dt.datetime | None = None,
            ) -> "IMessage":
                pass

        class FakeCommand(IMessage, metaclass=FakeCommandMeta):
            @property
            def __domain_name__(self) -> DomainName:
                pass

            @property
            def __message_name__(self) -> MessageName:
                pass

            @property
            def __type__(self) -> MessageType:
                return MessageType.COMMAND

            @property
            def __reference__(self) -> UUID:
                pass

            @property
            def __timestamp__(self) -> dt.datetime:
                pass

            @property
            def __payload__(self) -> dict:
                pass

            def to_dict(self) -> dict:
                pass

            def to_json(self) -> str:
                pass

        cmd = FakeCommand()
        assert isinstance(cmd, IMessage)
        assert isinstance(cmd, AbstractCommand)

    def test_event_instance(self):
        class FakeEvent(IMessage):
            @property
            def __domain_name__(self) -> DomainName:
                pass

            @property
            def __message_name__(self) -> MessageName:
                pass

            @property
            def __type__(self) -> MessageType:
                return MessageType.EVENT

            @property
            def __reference__(self) -> UUID:
                pass

            @property
            def __timestamp__(self) -> dt.datetime:
                pass

            @property
            def __payload__(self) -> dict:
                pass

            def to_dict(self) -> dict:
                pass

            def to_json(self) -> str:
                pass

        cmd = FakeEvent()
        assert isinstance(cmd, IMessage)
        assert isinstance(cmd, AbstractEvent)
