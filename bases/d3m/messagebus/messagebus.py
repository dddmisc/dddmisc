import asyncio
import contextlib
import functools
import inspect
import threading
import warnings
from contextlib import suppress
from typing import (
    TypeVar,
    AsyncContextManager,
    Callable,
    Coroutine,
    ContextManager,
    Any,
    Generator,
)

from d3m.core import (
    IHandlersCollection,
    IMessage,
    IMessagebus,
    IMessagebusPolicy,
    DomainName,
    MessagebusEvents,
    MessageType,
    helpers,
    AbstractCommand,
    AbstractEvent,
)
from d3m.core.abstractions import Context, AbstractCommandMeta

from .collection import MessagebusHandlersCollection
from .context import MessagebusContext
from .event_manager import MessagebusEventsManager

_MessagebusT = TypeVar("_MessagebusT", bound=IMessagebus)

Lifespan = Callable[[_MessagebusT], AsyncContextManager[None]]


class Messagebus(IMessagebus):
    """
    d3m.core.IMessagebus implementation

    Args:
        lifespan: async context manager entered when first run and exit after close

    Methods:
        subscribe: Subscribe a listener to one or more events of the message bus.
        unsubscribe: Unsubscribe a listener from one or more events of the message bus.
        run: Run the message bus.
        run_until_complete: Run the message bus until the execution of a specified command.
        stop: Stop the message bus.
        close: Close the message bus.
        is_running: Check if the message bus is running.
        is_closed: Check if the message bus is closed.
        include_collection: Include a collection of handlers in the message bus.
        handle_message: Handle a message and return a future representing the completion of the handling.
        get_context: Get the context of the current instance.
        get_registered_commands: Get commands from handlers collection.
        set_defaults: Set defaults for the domain's command's handlers.
    """

    def __init__(self, *, lifespan: Lifespan[_MessagebusT] | None = None):
        self._is_running = False
        self._is_closed = False
        self._collection = MessagebusHandlersCollection()
        self._context = MessagebusContext()
        self._tasks: dict[IMessage, asyncio.Future] = {}
        self._finish_tasks_event = asyncio.Event()
        self._finish_tasks_event.set()
        self._defaults: dict[DomainName, dict] = {}
        lifespan_context: AsyncContextManager = _build_lifespan(lifespan)(self)

        self._lifespan = _wrap_async_ctx_manager(lifespan_context)
        self._in_lifespan_context = False
        self._events_manager = MessagebusEventsManager(self)

    def get_registered_commands(self) -> Generator[AbstractCommandMeta, None, None]:
        for command in self._collection.get_registered_commands():
            yield command

    def subscribe(
        self,
        *events: MessagebusEvents,
        listener: Callable[[IMessagebus, MessagebusEvents], Coroutine],
    ):
        for event in events:
            self._events_manager.subscribe(event, listener)

    def unsubscribe(
        self,
        *events: MessagebusEvents,
        listener: Callable[[IMessagebus, MessagebusEvents], Coroutine],
    ):
        for event in events:
            self._events_manager.unsubscribe(event, listener)

    def get_context(self) -> Context:
        return Context(
            context_message=self._context.context_message,
        )

    async def run(self):
        if self.is_running():
            return
        self._raise_if_closed()
        if len(self._collection) == 0:
            raise RuntimeError("Messagebus cannot be run without handlers")
        await self._finish_tasks_event.wait()
        self._check_another_running_messagebus()
        await self._events_manager.notify(MessagebusEvents.BEFORE_RUN, self)
        self._collection.update_defaults()  # type: ignore
        if not self._in_lifespan_context:
            await anext(self._lifespan)
            self._in_lifespan_context = True
        self._is_running = True
        helpers.set_running_messagebus(self)
        await self._events_manager.notify(MessagebusEvents.AFTER_RUN, self)

    def _check_another_running_messagebus(self):
        try:
            other_mb = helpers.get_running_messagebus()
        except RuntimeError:
            pass
        else:
            if self is not other_mb:
                raise RuntimeError(
                    "Cannot run the messagebus while another messagebus is running"
                )

    async def run_until_complete(self, command: IMessage, **dependencies) -> Any:
        if command.__type__ != MessageType.COMMAND:
            raise TypeError(
                f"Invalid message type. "
                f"Expected {MessageType.COMMAND}, got {command.__type__}"
            )
        await self.run()
        future = self.handle_message(command, **dependencies)
        await self.stop()
        return await future

    async def stop(self):
        if self.is_running():
            await self._events_manager.notify(MessagebusEvents.BEFORE_STOP, self)
            self._is_running = False
            self._finish_tasks_event.clear()
            task = asyncio.create_task(self._wait_current_tasks())
            task.add_done_callback(self._stop_messagebus_done_cb)
            asyncio.ensure_future(task)
            await self._finish_tasks_event.wait()
            await self._events_manager.notify(MessagebusEvents.AFTER_STOP, self)
        else:
            await self._finish_tasks_event.wait()

    async def _wait_current_tasks(self):
        tasks = list(self._tasks.values())
        while tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            tasks = [task for task in self._tasks.values() if task.done() is False]

    def _stop_messagebus_done_cb(self, _task):
        helpers.set_running_messagebus(None)
        self._finish_tasks_event.set()

    async def close(self):
        notify = not self._is_closed
        self._is_closed = True
        await self.stop()
        if notify:
            await self._events_manager.notify(MessagebusEvents.BEFORE_CLOSE, self)
        with suppress(StopAsyncIteration):
            await anext(self._lifespan)
        if notify:
            await self._events_manager.notify(MessagebusEvents.AFTER_CLOSE, self)

    def is_running(self):
        return self._is_running

    def is_closed(self) -> bool:
        return self._is_closed

    def include_collection(self, collection: IHandlersCollection):
        self._collection.include_collection(collection)

    def handle_message(self, message: IMessage, **dependencies) -> asyncio.Future:
        if not self.is_running() and self._context.context_message is None:
            raise RuntimeError("Messagebus is not running")
        elif self.is_closed():
            # TODO: add warning
            self._raise_if_closed()

        if isinstance(message, AbstractCommand):
            return self._handle_command(message, dependencies)

        if isinstance(message, AbstractEvent):
            return self._handle_event(message, dependencies)

        raise TypeError(f"Unknown type of message {message.__type__!r}")

    def _handle_command(
        self, command: AbstractCommand, dependencies: dict
    ) -> asyncio.Future:
        with self._context as ctx:
            ctx.set_context_message(command)
            ctx.update_dependencies(dependencies)
            handler = self._collection.get_command_handler(command, **ctx.dependencies)

            future = asyncio.Future()  # type: asyncio.Future
            task = asyncio.create_task(handler())
            self._add_message(task, command)
            task.add_done_callback(
                functools.partial(self._remove_message, message=command)
            )

            task.add_done_callback(
                functools.partial(self._set_task_result, future=future)
            )
            return future

    def _handle_event(self, event: AbstractEvent, dependencies) -> asyncio.Future:
        with self._context as ctx:
            ctx.set_context_message(event)
            ctx.update_dependencies(dependencies)
            handlers = self._collection.get_event_handlers(event, **ctx.dependencies)
            tasks = []
            for handler in handlers:
                task = asyncio.create_task(handler())
                tasks.append(task)

            future: asyncio.Future = asyncio.Future()
            gather_future = asyncio.gather(*tasks, return_exceptions=True)
            self._add_message(gather_future, event)
            gather_future.add_done_callback(
                functools.partial(self._remove_message, message=event)
            )
            gather_future.add_done_callback(
                functools.partial(self._set_task_result, future=future)
            )
            return future

    def _remove_message(self, _task, /, message: IMessage):
        loop = asyncio.get_running_loop()
        loop.call_soon(self._tasks.__delitem__, message)

    def _add_message(self, task, /, message: IMessage):
        self._tasks[message] = task

    @staticmethod
    def _set_task_result(task: asyncio.Future, /, future: asyncio.Future):
        if task.exception() is not None:
            future.set_exception(task.exception())  # type: ignore
        else:
            future.set_result(task.result())

    def _raise_if_closed(self):
        if self.is_closed():
            raise RuntimeError("Messagebus is closed")

    def set_defaults(self, __domain: DomainName | str, /, **defaults):
        if self.is_running():
            raise RuntimeError("Can not set defaults to messagebus after run")
        self._collection.set_defaults(__domain, **defaults)


class _DefaultLifespan:
    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    def __call__(self, mb: object):
        return self


_T = TypeVar("_T")


async def _wrap_async_ctx_manager(context: AsyncContextManager):
    async with context:
        yield


class _AsyncLiftContextManager(AsyncContextManager[_T]):
    def __init__(self, cm: ContextManager[_T]):
        self._cm = cm

    async def __aenter__(self) -> _T:
        return self._cm.__enter__()

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool | None:
        return self._cm.__exit__(exc_type, exc_value, traceback)


def _wrap_gen_lifespan_context(
    lifespan_context: Callable[[Any], Generator]
) -> Callable[[Any], AsyncContextManager]:
    manager = contextlib.contextmanager(lifespan_context)

    @functools.wraps(manager)
    def wrapper(mb: Any) -> _AsyncLiftContextManager:
        return _AsyncLiftContextManager(manager(mb))

    return wrapper


def _build_lifespan(lifespan: Lifespan[_MessagebusT] | None) -> Lifespan:
    if lifespan is None:
        return _DefaultLifespan()

    elif inspect.isasyncgenfunction(lifespan):
        warnings.warn(
            "async generator function lifespans are deprecated, "
            "use an @contextlib.asynccontextmanager function instead",
            DeprecationWarning,
        )
        return contextlib.asynccontextmanager(lifespan)  # type: ignore[arg-type]
    elif inspect.isgeneratorfunction(lifespan):
        warnings.warn(
            "generator function lifespans are deprecated, "
            "use an @contextlib.asynccontextmanager function instead",
            DeprecationWarning,
        )
        return _wrap_gen_lifespan_context(lifespan)  # type: ignore[arg-type]

    else:
        return lifespan


class MessagebusPolicy(IMessagebusPolicy):
    """Default policy implementation for accessing the messagebus.

    In this policy, each thread has its own messagebus. However, we
    only automatically create a messagebus by default for the main
    thread; other threads by default have no event loop.

    Other policies may have different rules (e.g. a single global
    messagebus, or automatically creating a messagebus per thread, or
    using some other notion of context to which a messagebus is
    associated).

    Methods:
        get_messagebus: Get the messagebus for the current context.
        set_messagebus: Set the messagebus for the current context.
        new_messagebus: Create and return a new messagebus object, according to this policy's rules.

    """

    _messagebus_factory = Messagebus

    class _Local(threading.local):
        messagebus: IMessagebus | None = None
        set_called = False

    def __init__(self):
        self._local = self._Local()

    def get_messagebus(self) -> IMessagebus:
        if (
            self._local.messagebus is None
            and not self._local.set_called
            and threading.current_thread() is threading.main_thread()
        ) or (
            self._local.messagebus is not None and self._local.messagebus.is_closed()
        ):
            self.set_messagebus(self.new_messagebus())

        if self._local.messagebus is None:
            raise RuntimeError(
                f"There is not current messagebus in thread {threading.current_thread().name!r}."
            )
        return self._local.messagebus

    def set_messagebus(self, messagebus: IMessagebus | None):
        self._local.set_called = True
        if messagebus is not None and not isinstance(messagebus, IMessagebus):
            raise TypeError(
                f"messagebus must be an instance of "
                f'IMessagebus or None, not "{type(messagebus).__name__}"'
            )
        self._local.messagebus = messagebus

    def new_messagebus(self) -> Messagebus:
        return self._messagebus_factory()


if helpers.DEFAULT_MESSAGEBUS_POLICY is None:
    helpers.DEFAULT_MESSAGEBUS_POLICY = MessagebusPolicy
