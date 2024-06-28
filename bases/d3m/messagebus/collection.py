from contextlib import suppress
from copy import copy
from typing import Any, Coroutine, Callable, Generator

from d3m.core import (
    IHandlersCollection,
    AbstractCommand,
    AbstractEvent,
    DomainName,
)
from d3m.core.abstractions import AbstractCommandMeta


class MessagebusHandlersCollection(IHandlersCollection):
    def __init__(self):
        self._defaults: dict[DomainName, dict[str, Any]] = {}
        self._collections: set[IHandlersCollection] = set()
        self._origins_collections_ids: set[int] = set()

    def get_command_handler(
        self, __command: AbstractCommand, /, **dependencies
    ) -> Callable[[], Coroutine]:
        result = []
        for collection in self._collections:
            with suppress(Exception):
                result.append(collection.get_command_handler(__command, **dependencies))

        if len(result) == 1:
            return result[0]
        if len(result) == 0:
            raise RuntimeError(
                f"Handler for command "
                f'"{__command.__domain_name__}.{__command.__message_name__}" '
                f"not registered"
            )
        raise RuntimeError(
            f"More one handler registered for command "
            f'"{__command.__domain_name__}.{__command.__message_name__}"'
        )

    def get_event_handlers(
        self, __event: AbstractEvent, **dependencies
    ) -> tuple[Callable[[], Coroutine], ...]:
        result: list[Callable[[], Coroutine]] = []
        for collection in self._collections:
            with suppress(Exception):
                result.extend(collection.get_event_handlers(__event, **dependencies))
        return tuple(result)

    def get_registered_commands(self) -> Generator[AbstractCommandMeta, None, None]:
        for collection in self._collections:
            for command in collection.get_registered_commands():  # type: ignore[attr-defined]
                yield command

    def include_collection(self, collection: IHandlersCollection) -> None:
        if not isinstance(collection, IHandlersCollection):
            raise TypeError(
                f"Invalid collection type expected type "
                f"{IHandlersCollection!r} got {collection!r}"
            )
        if id(collection) in self._origins_collections_ids:
            return
        self._collections.add(copy(collection))
        self._origins_collections_ids.add(id(collection))

    def __len__(self):
        return len(self._collections)

    def set_defaults(self, __domain: DomainName | str, **defaults):
        self._defaults.setdefault(DomainName(__domain), {}).update(defaults)

    def update_defaults(self):
        for domain, defaults in self._defaults.items():
            for collection in self._collections:
                collection.set_defaults(domain, **defaults)
