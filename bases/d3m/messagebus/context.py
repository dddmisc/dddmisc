from contextvars import ContextVar  # noqa
from types import MappingProxyType
from uuid import uuid4

from d3m.core import IMessage


class MessagebusContext:
    def __init__(
        self,
        *,
        dependencies_ctx: ContextVar[dict | None] | None = None,
        context_message_ctx: ContextVar[IMessage | None] | None = None,
    ):
        self._dependencies_ctx = dependencies_ctx or ContextVar(str(uuid4()))
        self._dependencies_value = self._dependencies_ctx.get({})

        self._context_message_ctx = context_message_ctx or ContextVar(str(uuid4()))
        self._context_message = self._context_message_ctx.get(None)

        self._subcontext: MessagebusContext | None = None

    @property
    def dependencies(self) -> MappingProxyType:
        return MappingProxyType(self._dependencies_ctx.get({}) or {})

    @property
    def context_message(self) -> IMessage | None:
        return self._context_message_ctx.get(None)

    def set_context_message(self, message: IMessage):
        self._context_message_ctx.set(message)

    def update_dependencies(self, dependencies: dict):
        self._dependencies_ctx.set({**self.dependencies, **dependencies})

    def _clear(self):
        del self._dependencies_ctx
        del self._dependencies_value
        del self._context_message_ctx
        del self._context_message

    def __enter__(self):
        if self._subcontext is not None:
            raise RuntimeError("Exit from current context before enter")
        self._dependencies_value = self._dependencies_ctx.get({})
        self._context_message = self._context_message_ctx.get(None)
        self._subcontext = MessagebusContext(
            dependencies_ctx=self._dependencies_ctx,
            context_message_ctx=self._context_message_ctx,
        )
        return self._subcontext

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._subcontext is not None:
            self._subcontext.__exit__(exc_type, exc_val, exc_tb)
            self._subcontext._clear()
            self._subcontext = None
        self._dependencies_ctx.set(self._dependencies_value)
        self._context_message_ctx.set(self._context_message)
