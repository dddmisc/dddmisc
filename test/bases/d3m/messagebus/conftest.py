import typing as t
from collections import ChainMap, defaultdict
from typing import Callable, Coroutine, Generator
import logging
import io

import pytest
from d3m import core as c
from d3m.core import DomainName
from d3m.core.abstractions import AbstractCommandMeta


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


Handler = t.Callable[[c.IMessage, t.ParamSpecKwargs], t.Coroutine]


@pytest.fixture
def handler_collection_factory():
    class FakeHandlersCollection(c.IHandlersCollection):
        def __init__(self):
            self._command_handlers: dict[
                tuple[c.DomainName, c.MessageName], Handler
            ] = {}
            self._event_handlers: dict[
                tuple[c.DomainName, c.MessageName], set[Handler]
            ] = defaultdict(set)
            self._is_frozen = False
            self._defaults = {}

        def add_handler(self, message_class: c.IMessageMeta, handler: Handler):
            key = (message_class.__domain_name__, message_class.__message_name__)
            if message_class.__type__ == "COMMAND":
                self._command_handlers[key] = handler
            elif message_class.__type__ == "EVENT":
                self._event_handlers[key].add(handler)

        def get_command_handler(
            self, __command: c.IMessage, /, **dependencies
        ) -> Callable[[], Coroutine]:
            handler = self._command_handlers[
                (__command.__domain_name__, __command.__message_name__)
            ]
            return self._build_wrapper(__command, handler, **dependencies)

        def get_event_handlers(
            self, __event: c.IMessage, /, **dependencies
        ) -> tuple[Callable[[], Coroutine], ...]:
            handlers = self._event_handlers[
                (__event.__domain_name__, __event.__message_name__)
            ]
            result = []
            for handler in handlers:
                result.append(self._build_wrapper(__event, handler, **dependencies))

            return tuple(result)  # noqa

        def get_registered_commands(self) -> Generator[AbstractCommandMeta, None, None]:
            for handler in self._command_handlers.values():
                yield handler

        def _build_wrapper(self, message, handler, **kwargs):
            async def wrapper():
                return await handler(
                    message,
                    **ChainMap(self._defaults.get(message.__domain_name__, {}), kwargs),
                )

            return wrapper

        @property
        def defaults(self):
            return self._defaults

        def set_defaults(self, __domain: DomainName | str, **defaults):
            self._defaults.setdefault(__domain, {}).update(defaults)

    def factory():
        return FakeHandlersCollection()

    return factory


@pytest.fixture(autouse=True)
async def stop_running_messagebus_after_test():
    yield
    try:
        messagebus = c.get_running_messagebus()
    except RuntimeError:
        pass
    else:
        await messagebus.stop()
        await messagebus.close()
