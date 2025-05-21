import importlib
import pkgutil
import sys

from .types import DomainName, MessagebusEvents, MessageName, MessageType
from .abstractions import (
    IDefaultDependency,
    IHandlersCollection,
    IMessage,
    AbstractEvent,
    AbstractCommand,
    IEventsOfMessagebusManager,
    IMessagesPublisher,
    IMessagebus,
    IMessagebusPolicy,
    IMessageMeta,
    IRegisteredCommands,
    IEntity,
    Version,
    IRootEntity,
)
from .helpers import (
    get_messagebus,
    get_messagebus_policy,
    get_running_messagebus,
    new_messagebus,
    set_messagebus,
    set_messagebus_policy,
)
from .message import UniversalMessage

__all__ = [
    # types
    "MessagebusEvents",
    "MessageType",
    "DomainName",
    "MessageName",
    "UniversalMessage",
    # abstractions
    "IMessageMeta",
    "IMessage",
    "AbstractCommand",
    "AbstractEvent",
    "IDefaultDependency",
    "IHandlersCollection",
    "IEventsOfMessagebusManager",
    "IMessagesPublisher",
    "IMessagebus",
    "IMessagebusPolicy",
    "IEntity",
    "IRegisteredCommands",
    "Version",
    "IRootEntity",
    # tools
    "get_messagebus_policy",
    "set_messagebus_policy",
    "get_messagebus",
    "set_messagebus",
    "new_messagebus",
    "get_running_messagebus",
]

[
    importlib.import_module(module.name)
    for module in pkgutil.iter_modules(sys.modules["d3m"].__path__, "d3m.")
]
