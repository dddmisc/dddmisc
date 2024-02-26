import datetime as dt
import json
from dataclasses import dataclass, field, asdict
from uuid import UUID, uuid4
import typing as t
import logging
import io

import pytest
from d3m.core import (
    IMessageMeta,
    IMessage,
    MessageType,
    MessageName,
    DomainName,
    AbstractCommand,
)
from d3m.core.abstractions import AbstractCommandMeta
from d3m.core.types import FrozenJsonDict


@pytest.fixture
def list_logger_handler() -> (
    t.Callable[[t.Union[str, logging.Logger]], t.List[logging.LogRecord]]
):
    """
    Utility method for creating a logger handler that captures log records.

    Returns a callable that can be used to retrieve the log records captured by the handler.
    """

    class ListHandler(logging.StreamHandler):
        def __init__(self, log_level: t.Literal[10] = logging.NOTSET, stream=None):
            self.record_history: t.List[logging.LogRecord] = []
            super(ListHandler, self).__init__(stream)
            self.setLevel(log_level)

        def handle(self, record: logging.LogRecord) -> bool:
            self.record_history.append(record)
            return super(ListHandler, self).handle(record)

    loggers: t.List[t.Tuple[logging.Logger, ListHandler, t.Any, bool]] = []

    def get_logger(logger: t.Union[str, logging.Logger]):
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        logger_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger_propagate = logger.propagate
        logger.propagate = False
        new_handler = ListHandler(logging.DEBUG, io.StringIO())
        loggers.append((logger, new_handler, logger_level, logger_propagate))
        logger.addHandler(new_handler)
        return new_handler.record_history

    yield get_logger
    for log, handler, level, propagate in loggers:
        if handler in log.handlers:
            log.handlers.remove(handler)
        log.setLevel(level)
        log.propagate = propagate


@pytest.fixture(autouse=True)
def clear_registered_handlers():
    from d3m.hc import HandlersCollection

    registered = HandlersCollection._COMMAND_HANDLERS_MAP  # noqa
    HandlersCollection._COMMAND_HANDLERS_MAP = {}
    yield
    HandlersCollection._COMMAND_HANDLERS_MAP = registered


@pytest.fixture
def command_class_builder():
    def builder(
        name="TestCommand", domain="test", annotation: dict = None
    ) -> IMessageMeta:
        class CommandMeta(AbstractCommandMeta):
            @property
            def __domain_name__(cls) -> DomainName:
                return DomainName(domain)

            @property
            def __message_name__(cls) -> MessageName:
                return MessageName(name)

            @property
            def __type__(cls) -> MessageType:
                return MessageType.COMMAND

            def load(
                cls,
                payload: t.Mapping | str | bytes,
                reference: UUID | None = None,
                timestamp: dt.datetime | None = None,
            ) -> "IMessage":
                fields = cls.__dataclass_fields__.keys()
                new_payload = {
                    key: value for key, value in payload.items() if key in fields
                }
                obj = cls(**new_payload)
                obj._reference = reference or obj._reference
                obj._timestamp = timestamp or obj._timestamp
                return obj

        class BaseCommand(AbstractCommand, metaclass=CommandMeta):
            _reference: UUID = field(default_factory=uuid4)
            _timestamp: dt.datetime = field(
                default_factory=lambda: dt.datetime.now(dt.timezone.utc)
            )

            @property
            def __domain_name__(self) -> DomainName:
                return DomainName(domain)

            @property
            def __message_name__(self) -> MessageName:
                return MessageName(name)

            @property
            def __reference__(self) -> UUID:
                return self._reference

            @property
            def __timestamp__(self) -> dt.datetime:
                return self._timestamp

            @property
            def __payload__(self) -> dict:
                return asdict(self)

            def to_dict(self) -> dict:
                return FrozenJsonDict(self.__payload__)

            def to_json(self) -> str:
                return json.dumps(self.to_dict())

        annotation = annotation or {}

        cls = type(BaseCommand)(
            name,
            (BaseCommand,),
            {
                "__module__": __name__,
                "__qualname__": "TestCommandHandlerDecorator.make_command_class.<locals>",
                "__annotations__": annotation,
            },
        )
        return dataclass(cls)

    return builder
