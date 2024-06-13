import logging
from copy import copy
from types import MappingProxyType
from typing import Any, Mapping, Callable, Coroutine, Iterable, Generator

import tenacity as tc
from d3m.core import (
    IHandlersCollection,
    DomainName,
    MessageName,
    AbstractCommand,
    AbstractEvent,
)
from d3m.core.abstractions import AbstractCommandMeta

from .command_handler import ICommandHandler, CommandHandler
from .conditions import ICondition, none_condition
from .subscribe_config import SubscribeConfig, retry_base, stop_base, wait_base
from ..core.helpers import parse_full_message_name

logger = logging.getLogger(str(__name__).rsplit(".", 1)[0])


class HandlersCollection(IHandlersCollection):
    """
    Class representing a collection of command and event handlers.

    The HandlersCollection class provides the following functionality:
        - Registration and retrieval of command handlers
        - Retrieval of event handlers
        - Registration of event subscriptions

    Args:
        name (str): The name of the collection

    Methods:
        get_command_handler: Retrieves the command handler for the specified command.
        get_event_handlers: Retrieves the event handlers for the specified event.
        get_registered_commands: Retrives registered handler commands.
        register: Registers a command handler.
        subscribe: Registers an event subscription.
        set_defaults: Sets default values for the specified domain.
        __copy__: Creates and returns a copy of the HandlersCollection object.

    Examples:
        **Init collection:**
        >>> collection = HandlersCollection(name='Example')
        >>> collection
        d3m.hc.HandlersCollection<name='Example', registered_command=0, registered_events=0>

        **Register command's handler:**
        >>> @collection.register
        ... async def command_handler1(cmd: CustomCommand1, dependencies1: int):
        ...     ...
        >>> @collection.register
        ... async def command_handler2(dependencies1: int, cmd: CustomCommand2):
        ...     ...
        >>>
        >>> collection
        d3m.hc.HandlersCollection<name='Example', registered_command=2, registered_events=0>

        Handlers collection associate command with command's argument by signature type hinting!

        **Subscribe to event:**
        >>> @collection.subscribe('other-domain.CustomEvent')
        ... @collection.register
        ... async def command_handler(arg1: str, cmd: CustomCommand):
        ...     return cmd.__reference__
        >>> collection
        d3m.hc.HandlersCollection<name='Example', registered_command=1, registered_events=1>


    """

    _COMMAND_HANDLERS_MAP: dict[tuple[DomainName, MessageName], ICommandHandler] = {}

    def __init__(self, *, name: str | None = None):
        self._name = name
        self._class_name = (
            f"{self.__module__.rsplit('.', 1)[0]}.{self.__class__.__name__}"
        )
        self._command_handlers: dict[
            tuple[DomainName, MessageName], ICommandHandler
        ] = {}
        self._events_configs: dict[
            tuple[DomainName, MessageName], set[SubscribeConfig]
        ] = {}
        self._defaults: dict[DomainName, dict[str, Any]] = {}

    def get_command_handler(
        self, __command: AbstractCommand, **dependencies
    ) -> Callable[[], Coroutine]:
        """
        Retrieves the command handler for the specified command.

        Args:
            __command: The command to be handled.
            **dependencies: Additional dependencies that the command handler may require.

        Returns:
            A callable that is a coroutine and represents the handler for the given command.

        Examples:
            >>> collection = HandlersCollection(name='Example')
            >>> @collection.register
            ... async def command_handler(arg1: str, cmd: CustomCommand):
            ...     return cmd.__reference__, arg1
            >>> cmd = CustomCommand()
            >>> handler = collection.get_command_handler(cmd, arg1='abc')
            >>> assert await handler() == (cmd.__reference__, 'abc')
        """
        handler = self._command_handlers.get(
            (__command.__domain_name__, __command.__message_name__)
        )
        if handler is None:
            raise RuntimeError(
                f"Handler for command "
                f'"{__command.__domain_name__}.{__command.__message_name__}" '
                f"not registered in collection"
            )

        if not isinstance(__command, handler.command_class):
            __command = handler.command_class.load(
                __command.__payload__, __command.__reference__, __command.__timestamp__
            )
        return handler.with_defaults(**dependencies).with_command(__command)

    def get_event_handlers(
        self, __event: AbstractEvent, /, **dependencies
    ) -> tuple[Callable[[], Coroutine], ...]:
        """
        Retrieves the event handlers for the specified event.

        Args:
            __event: An instance of AbstractEvent representing the event.
            **dependencies: Additional dependencies required by the event handlers.

        Returns:
            (tuple[Callable[[], Coroutine], ...]): A tuple of callables that represent the event handlers for the given event.
                Each callable is a coroutine that takes no arguments.

        Examples:
            >>> collection = HandlersCollection(name='Example')
            >>> @collection.subscribe('other-domain.CustomEvent')
            >>> @collection.register
            ... async def command_handler1(arg1: str, cmd: CustomCommand1):
            ...     return arg1
            >>> @collection.subscribe('other-domain.CustomEvent')
            >>> @collection.register
            ... async def command_handler2(arg2: str, cmd: CustomCommand2):
            ...     return arg2
            >>> event = UniversalMessage('other-domain.CustomEvent', 'event', {})
            >>> handlers = collection.get_event_handlers(event, arg1='abc', arg2='xyz')
            >>> assert len(handlers) == 2
            >>> assert await handlers[0]() == 'abc'
            >>> assert await handlers[1]() == 'xyz'
        """
        result = []
        event_key = __event.__domain_name__, __event.__message_name__
        for cfg in self._events_configs.get(event_key, ()):
            cmd_handler = self._command_handlers.get(cfg.command_key)
            try:
                if cmd_handler is not None and (
                    event_handler := cfg.build_handler(
                        __event,
                        cmd_handler,
                        **dependencies,
                    )
                ):
                    result.append(event_handler)
            except:  # noqa
                logger.exception(
                    'Fail build handler for event "%s.%s"',
                    __event.__domain_name__,
                    __event.__message_name__,
                    extra={"payload": __event.to_dict()},
                )
        return tuple(result)

    def get_registered_commands(self) -> Generator[AbstractCommandMeta, None, None]:
        for handler in self._command_handlers.values():
            yield handler.command_class

    def register(self, func: Callable[..., Coroutine]) -> ICommandHandler:
        """
        Registers a command handler.

        Args:
            func: A callable object that takes any number of arguments and returns a coroutine.

        Returns:
            (ICommandHandler): An instance of `ICommandHandler` representing the registered handler.
        """
        handler = CommandHandler().set_function(func)
        self._register_handler(handler)
        return handler

    def _register_handler(self, handler: ICommandHandler):
        command_class = handler.command_class
        command_key = (command_class.__domain_name__, command_class.__message_name__)

        if command_key in self._COMMAND_HANDLERS_MAP:
            raise RuntimeError(
                f"Handler for command {command_class!r} already registered"
            )
        handler = self._set_handler_defaults(handler)
        self._command_handlers[command_key] = handler
        self._COMMAND_HANDLERS_MAP[command_key] = handler

    def _set_handler_defaults(self, handler: ICommandHandler) -> ICommandHandler:
        domain = handler.command_class.__domain_name__
        defaults = self._defaults.get(domain, {})
        return handler.with_defaults(**defaults)

    def subscribe(
        self,
        full_event_name: str,
        /,
        *,
        condition: ICondition = none_condition,
        converter: Callable[[Mapping], Mapping] = lambda payload: payload,
        retry: retry_base = tc.retry_never,
        stop: stop_base = tc.stop_after_attempt(1),
        wait: wait_base = tc.wait_none(),
    ) -> Callable[[ICommandHandler], ICommandHandler]:
        """
        Registers an event subscription.

        Args:
            full_event_name: A string representing the full name of the event.
            condition: An instance of ICondition. (Default: none_condition)
            converter: A Callable object using for conversion event's payload to command's payload.
                Can used as Anti-coraption layer between events from other domain.
                (Default: lambda payload: payload)
            retry: retry exec event's handler strategy. (Default: tenacity.retry_never)
            stop: stop retry strategy. (Default: tenacity.stop_after_attempt(1))
            wait: wait between attempts strategy. (Default: tenacity.wait_none())

        Returns:
            (Callable[[ICommandHandler], ICommandHandler]): A ICommandHandler's decorator.
                Not change handler behaviour.
        """
        return _Subscriber(
            self._register_subscription,
            full_event_name=full_event_name,
            kwargs=MappingProxyType(
                {
                    "condition": condition,
                    "converter": converter,
                    "retry": retry,
                    "stop": stop,
                    "wait": wait,
                }
            ),
        )

    def _register_subscription(
        self,
        full_event_name: str,
        command_class: AbstractCommandMeta,
        kwargs: dict,
    ):
        domain, name = parse_full_message_name(full_event_name)
        config = SubscribeConfig(command_class, domain, name, **kwargs)
        if config.command_key not in self._command_handlers:
            raise RuntimeError(
                f"Handler for command ({config.command_key[0]}.{config.command_key[1]}) "
                f"not registered in collection ({self})"
            )
        self._events_configs.setdefault(config.event_key, set()).add(config)

    def __repr__(self):
        return (
            f"{self._class_name}<name={self._name!r}, "
            f"registered_command={len(self._command_handlers)}, "
            f"registered_events={len(self._events_configs)}>"
        )

    def set_defaults(self, __domain: str | DomainName, /, **defaults):
        """
        Sets default values for handler's dependencies.
        Dependencies associated with handlers by domain and attribute name.

        Args:
            __domain: The domain for which default values are being set. Can be either a string or a DomainName object.
            **defaults: Dependencies values for the specified domain.

        """
        __domain = DomainName(__domain)
        self._defaults.setdefault(__domain, {}).update(defaults)
        for key, handler in self._command_handlers.items():
            domain, _ = key
            if domain == __domain:
                self._command_handlers[key] = self._set_handler_defaults(handler)

    def __copy__(self):
        new_collection = type(self)(name=self._name)

        new_collection._command_handlers = copy(self._command_handlers)
        new_collection._events_configs = {
            event_key: copy(subscribers)
            for event_key, subscribers in self._events_configs.items()
        }

        for domain, defaults in self._defaults.items():
            new_collection._defaults[domain] = copy(defaults)

        return new_collection


class _Subscriber:
    def __init__(self, callback: Callable, full_event_name: str, kwargs: Mapping):
        self._callback = callback
        self._full_event_name = full_event_name
        self._kwargs = kwargs

    def __call__(self, handler: ICommandHandler):
        self._callback(self._full_event_name, handler.command_class, self._kwargs)
        return handler
