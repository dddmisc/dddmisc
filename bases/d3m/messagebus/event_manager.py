import asyncio
from contextlib import suppress
import logging
from typing import Callable, Coroutine

from d3m.core import MessagebusEvents, IMessagebus

MessagebusListener = Callable[[IMessagebus, MessagebusEvents], Coroutine]

logger = logging.getLogger(str(__name__).rsplit(".", 1)[0])


class MessagebusEventsManager:
    def __init__(self, messagebus: IMessagebus):
        self._messagebus = messagebus
        self._listeners: dict[MessagebusEvents, set[MessagebusListener]] = {}

    def subscribe(self, event: MessagebusEvents, listener: MessagebusListener):
        self._listeners.setdefault(event, set()).add(listener)

    def unsubscribe(self, event: MessagebusEvents, listener: MessagebusListener):
        with suppress(KeyError):
            self._listeners.get(event, set()).remove(listener)

    async def notify(self, event: MessagebusEvents, messagebus: IMessagebus):
        assert messagebus is self._messagebus
        listeners = list(self._listeners.get(event, set()))
        tasks = [listener(messagebus, event) for listener in listeners]
        if tasks:
            result = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            result = []
        for i, item in enumerate(result):
            if isinstance(item, Exception):
                logger.error(
                    "Fail exec %s listener with %s event",
                    listeners[i],
                    event,
                    exc_info=(type(item), item, item.__traceback__),
                    stack_info=True,
                )
