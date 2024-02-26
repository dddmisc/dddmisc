from typing import Callable, Mapping

import tenacity as tc
from d3m.core import (
    DomainName,
    MessageName,
    MessageType,
    AbstractEvent,
    AbstractCommand,
)
from tenacity.retry import retry_base
from tenacity.stop import stop_base
from tenacity.wait import wait_base

from .command_handler import ICommandHandler
from .conditions import ICondition, none_condition
from d3m.core.abstractions import AbstractCommandMeta


class SubscribeConfig:
    def __init__(
        self,
        command: AbstractCommandMeta,
        event_domain: str | DomainName,
        event_name: str | MessageName,
        *,
        condition: ICondition = none_condition,
        converter: Callable[[Mapping], Mapping] = lambda x: x,
        retry: retry_base = tc.retry_never,
        stop: stop_base = tc.stop_after_attempt(1),
        wait: wait_base = tc.wait_none(),
    ):
        self._command = command
        self._event_domain = DomainName(event_domain)
        self._event_name = MessageName(event_name)
        self._condition = condition
        self._converter = converter
        self._retry = tc.AsyncRetrying(stop=stop, wait=wait, retry=retry, reraise=True)

    @property
    def command_key(self) -> tuple[DomainName, MessageName]:
        return self._command.__domain_name__, self._command.__message_name__

    @property
    def event_key(self) -> tuple[DomainName, MessageName]:
        return self._event_domain, self._event_name

    def build_handler(
        self,
        __event: AbstractEvent,
        __handler: ICommandHandler,
        /,
        **dependencies,
    ) -> ICommandHandler | None:
        if __handler.command_class != self._command:
            raise ValueError(
                f"Handler not register other command class, "
                f"expected {self._command!r} got {__handler.command_class!r}"
            )
        if (
            __event.__type__ != MessageType.EVENT
            or __event.__domain_name__ != self._event_domain
            or __event.__message_name__ != self._event_name
        ):
            raise ValueError(f"Can not build handler from {__event!r}")
        if self._condition.check(__event) is False:
            return None
        command = self._build_command(__event)
        retry = self._retry.copy()
        return retry.wraps(
            __handler.with_defaults(**dependencies).with_command(command)
        )

    def _build_command(self, event: AbstractEvent) -> AbstractCommand:
        payload = self._converter(event.to_dict())
        return self._command.load(payload)
