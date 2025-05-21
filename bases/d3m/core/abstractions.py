import abc
import asyncio
import datetime as dt
from typing import (
    Coroutine,
    Mapping,
    Any,
    Callable,
    Generator,
    Generic,
    Type,
    TypeVar,
    NewType,
    Iterable,
)
from typing import TypedDict
from uuid import UUID
from pydantic import BaseModel

from .types import DomainName, MessagebusEvents, MessageName, MessageType


class IMessageMeta(abc.ABCMeta):
    """
    This class is a metaclass for defining message classes.
    It is meant to be inherited by a message class definitions.

    Attributes:
        __domain_name__: The name of the message domain. Should be of type DomainName.
        __message_name__: The name of the message. Should be of type MessageName.
        __payload_model__: The payload model of the message.
        __type__: The type of the message. Should be of type MessageType.

    Methods:
         load: Constructs a new message object.
    """

    @property
    @abc.abstractmethod
    def __domain_name__(cls) -> DomainName:
        """Get the domain name of the message.

        Returns:
            DomainName: The name of the message domain.
        """

    @property
    @abc.abstractmethod
    def __message_name__(cls) -> MessageName:
        """
        Get the name of the message.

        Returns:
            MessageName: The name of the message.
        """

    @property
    @abc.abstractmethod
    def __payload_model__(cls) -> Type[BaseModel]:
        """
        Get the payload model of the message.

        Returns:
            Type[BaseModel]: The payload model of the message.
        """

    @property
    @abc.abstractmethod
    def __type__(cls) -> MessageType:
        """
        Get the type of the message.

        Returns:
            MessageType: The MessageType of the message.
        """

    @abc.abstractmethod
    def load(
        cls,
        payload: Mapping | str | bytes,
        reference: UUID | None = None,
        timestamp: dt.datetime | None = None,
    ) -> "IMessage":
        """
        Constructs a new message object.

        Args:
            payload (Union[Mapping, str, bytes]): The payload of the message.
                Any of a dictionary, json string, or bytes.

            reference (Optional[UUID]): The reference identifier of the message.
                If not provided, a new UUID will be generated.

            timestamp (Optional[datetime]): The creation timestamp of the message.
                If not provided, the current time will be used.

        Returns:
            IMessage: The newly created message object.
        """


class IMessage(abc.ABC, metaclass=IMessageMeta):
    """
    Metaclass: `IMessageMeta`

    Abstract base class for defining message objects.

    Attributes:
        __domain_name__ (DomainName): The name of the subject area.
        __message_name__ (MessageName): The name of the message.
        __type__ (MessageType): The type of the message.
        __reference__ (UUID): The identifier of the message.
        __timestamp__ (datetime.datetime): The creation time of the message.
        __payload__ (dict): The payload of the message.

    Methods:
        load: Constructs a new message object.
        to_dict: Returns a JSON serializable dictionary of the payload.
        to_json: Returns a JSON string representation of the payload.
    """

    @property
    @abc.abstractmethod
    def __domain_name__(self) -> DomainName:
        """
        Get the domain name of the message.

        Returns:
            DomainName: The name of the message domain.
        """

    @property
    @abc.abstractmethod
    def __message_name__(self) -> MessageName:
        """
        Get the name of the message.

        Returns:
            MessageName: The name of the message.
        """

    @property
    @abc.abstractmethod
    def __type__(self) -> MessageType:
        """
        Get the type of the message.

        Returns:
            MessageType: The MessageType of the message.
        """

    @property
    @abc.abstractmethod
    def __reference__(self) -> UUID:
        """
        Get the reference of the message.

        Returns:
            UUID: The reference of the message.
        """

    @property
    @abc.abstractmethod
    def __timestamp__(self) -> dt.datetime:
        """
        Get the creation time of the message

        Returns:
            datetime.datetime: The creation of the message with timezone information.
        """

    @property
    @abc.abstractmethod
    def __payload__(self) -> dict:
        """
        Get the payload of the message
        Returns:
            dict: The payload of the message.
        """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        Converts the message's payload to a json-serializable dictionary.


        Returns:
            dict: A dictionary representation of the message's payload.

        """

    @abc.abstractmethod
    def to_json(self) -> str:
        """
        Converts the message's payload to a json string.


        Returns:
            str: A json string representation of the message's payload.

        """

    def __eq__(self, other: Any) -> bool:
        """
        Checks for equality between two IMessage objects.
        Compares the current object with another object to determine if they are equal.
        The objects are considered equal if:
        - other is an instance of IMessage
        - self and other have identical values for the attributes __type__, __domain_name__, __message_name__, and __reference__
        Args:
            other (Any): The object to compare with the current object.
        Returns:
            bool: True if the objects are equal, otherwise False.
        """
        return (
            isinstance(other, IMessage)
            and self.__type__ == other.__type__
            and self.__domain_name__ == other.__domain_name__
            and self.__message_name__ == other.__message_name__
            and self.__reference__ == other.__reference__
        )

    def __hash__(self):
        """
        Returns the hash value of the IMessage object.
        The hash value is computed based on the __reference__ attribute. This method
        allows IMessage objects to be used in hash tables and other data structures
        that rely on hashing.
        Returns:
            int: The hash value of the IMessage object.
        """
        return hash(
            f"{self.__type__}: {self.__message_name__}@{self.__domain_name__}({self.__reference__})"
        )


class AbstractCommandMeta(IMessageMeta, abc.ABCMeta):
    @property
    def __type__(cls) -> MessageType:
        return MessageType.COMMAND

    def __instancecheck__(cls, __instance):
        if cls is AbstractCommand:
            return (
                isinstance(__instance, (IMessage, IMessageMeta))
                and __instance.__type__ == MessageType.COMMAND
            )
        else:
            return super().__instancecheck__(__instance)

    @abc.abstractmethod
    def load(
        cls,
        payload: Mapping | str | bytes,
        reference: UUID | None = None,
        timestamp: dt.datetime | None = None,
    ) -> "AbstractCommand":
        pass


class AbstractCommand(IMessage, abc.ABC, metaclass=AbstractCommandMeta):
    """
    This class represents an abstract command.

    Attributes:
        __domain_name__ (DomainName): The name of the subject area.
        __message_name__ (MessageName): The name of the message.
        __type__ (MessageType): The type of the message.
        __reference__ (UUID): The identifier of the message.
        __timestamp__ (datetime.datetime): The creation time of the message.
        __payload__ (dict): The payload of the message.

    Methods:
        load: Constructs a new message object.
        to_dict: Returns a JSON serializable dictionary of the payload.
        to_json: Returns a JSON string representation of the payload.
    """

    @property
    def __type__(self) -> MessageType:
        """
        Get the type of the command.

        Returns:
            MessageType: `MessageType.COMMAND`.
        """
        return MessageType.COMMAND


class AbstractEventMeta(IMessageMeta, abc.ABCMeta):
    @property
    def __type__(cls) -> MessageType:
        return MessageType.EVENT

    def __instancecheck__(cls, __instance=None):
        if cls is AbstractEvent:
            return (
                isinstance(__instance, IMessage)
                and __instance.__type__ == MessageType.EVENT
            )
        return super().__instancecheck__(__instance)

    @abc.abstractmethod
    def load(
        cls,
        payload: Mapping | str | bytes,
        reference: UUID | None = None,
        timestamp: dt.datetime | None = None,
    ) -> "AbstractEvent":
        pass


class AbstractEvent(IMessage, abc.ABC, metaclass=AbstractEventMeta):
    """
    This class represents an abstract event.

    Attributes:
        __domain_name__ (DomainName): The name of the subject area.
        __message_name__ (MessageName): The name of the message.
        __type__ (MessageType): The type of the message.
        __reference__ (UUID): The identifier of the message.
        __timestamp__ (datetime.datetime): The creation time of the message.
        __payload__ (dict): The payload of the message.

    Methods:
        load: Constructs a new message object.
        to_dict: Returns a JSON serializable dictionary of the payload.
        to_json: Returns a JSON string representation of the payload.
    """

    @property
    def __type__(self) -> MessageType:
        """
        Get the type of the command.

        Returns:
            MessageType: MessageType.EVENT.
        """
        return MessageType.EVENT


class Context(TypedDict):
    context_message: IMessage | None


class IRegisteredCommands(abc.ABC):
    """
    Interface for getting commands from registered handlers.

    This interface provides a method `get_registered_commands`
    that allows users to get commands from registered handlers.
    """

    @abc.abstractmethod
    def get_registered_commands(self) -> Generator[AbstractCommandMeta, None, None]:
        """
        Get commands from registered handlers.

        Returns:
            Generator[AbstractCommandMeta, None, None]: commands from registered handlers
        """


class IDefaultDependency(abc.ABC):
    """
    Abstract class for setting default dependencies for domain command handlers.

    This class provides a method `set_defaults` that allows users to set default dependencies for command handlers associated with a specific domain.

    """

    @abc.abstractmethod
    def set_defaults(self, __domain: DomainName | str, **defaults) -> None:
        """
        Set defaults dependencies for domain command handlers

        Args:
            __domain:
            **defaults:
        """


class IHandlersCollection(IRegisteredCommands, IDefaultDependency, abc.ABC):
    """
    IHandlersCollection is an abstract base class that defines the interface for a collection of message handlers.

    Methods:
        get_command_handler: Get the registered handler for a command.

        get_event_handlers: Get the registered handlers for commands caused by an event.

    """

    @abc.abstractmethod
    def get_command_handler(
        self, __command: AbstractCommand, /, **dependencies
    ) -> Callable[[], Coroutine]:
        """
        Get registered command's handler for the command

        Args:
            __command (AbstractCommand): domain's command object
            dependencies: dependencies for command's handler

        Returns:
             Callable[[], Coroutine]: command's handler.
        """

    @abc.abstractmethod
    def get_event_handlers(
        self, __event: AbstractEvent, /, **dependencies
    ) -> tuple[Callable[[], Coroutine], ...]:
        """
        Get registered command's handlers for commands caused to event

        Args:
            __event (AbstractEvent): domain's event object
            dependencies: dependencies for the event's handlers.

        Returns:
            (tuple[Callable[[], Coroutine], ...]): event's handlers
        """


class IEventsOfMessagebusManager(abc.ABC):
    @abc.abstractmethod
    def subscribe(
        self,
        *events: MessagebusEvents,
        listener: Callable[["IMessagebus", MessagebusEvents], Coroutine],
    ):
        """
        Subscribe a listener to one or more events of messagebus

        Args:
            *events (MessagebusEvents):
                A variable number of MessagebusEvents objects representing the events
                that the listener will subscribe to.

            listener (Callable[[IMessagebus, MessagebusEvents], Coroutine]):
                A callable object that takes two arguments:
                    1. An instance of the IMessagebus interface.
                    2. A MessagebusEvents object representing the event that occurred.
                This callable object should return a coroutine.

        """

    @abc.abstractmethod
    def unsubscribe(
        self,
        *events: MessagebusEvents,
        listener: Callable[["IMessagebus", MessagebusEvents], Coroutine],
    ):
        """
        Unsubscribe a listener from one or more events.

        Args:
            *events (MessagebusEvents): One or more events to unsubscribe the listener from.

            listener (Callable[[IMessagebus, MessagebusEvents], Coroutine]):
                The listener to unsubscribe from the events.

        """


class IMessagesPublisher(abc.ABC):
    @abc.abstractmethod
    def handle_message(self, message: IMessage, **dependencies) -> asyncio.Future:
        """
        Handles a message and returns a future representing the completion of the message handling.

        Args:
            message (IMessage): The message to be handled. Must implement the `IMessage` interface.
            **dependencies: Additional dependencies required for handling the message.

        Returns:
            asyncio.Future: An asyncio future representing the completion of the message handling.
        """


class IMessagebus(
    IMessagesPublisher,
    IEventsOfMessagebusManager,
    IRegisteredCommands,
    IDefaultDependency,
    abc.ABC,
):
    """
    Represents the interface for a message bus.

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
        set_defaults: Set defaults for the domain's command's handlers.

    """

    @abc.abstractmethod
    async def run(self) -> None:
        """
        Run the messagebus.
        This method calls before-run and after-run events subscribed listeners.
        This method should be idempotent behaviour.

        Raises:
            RuntimeError: if messagebus is closed.
        """

    @abc.abstractmethod
    async def run_until_complete(self, command: AbstractCommand, **dependencies) -> Any:
        """
        Runs the messagebus until given command's handler execution.

        Args:
            command (AbstractCommand): A IMessage object representing the command to be executed.
            **dependencies (Any): Additional dependencies that may be required by the command.

        Returns:
            Any: The result of the command execution, which may vary depending on the command.

        """

    @abc.abstractmethod
    async def stop(self) -> None:
        """
        Stops the messagebus.
        This method blocked handle the message to messagebus,
        exclude the messages generated by current messages handlers.
        Stops the current operation.
        This method should call before-stop and after-stop events subscribed listeners.
        This method should be idempotent call behaviour.
        This method return after finished all messages handlers.
        """

    @abc.abstractmethod
    async def close(self) -> None:
        """
        Close the messagebus.
        This method blocked handle any message to messagebus.
        This method should call before-close and after-close events subscribed listeners.
        """

    @abc.abstractmethod
    def is_running(self):
        """
        Check if the messagebus is running.

        This method returns a boolean value indicating whether the messagebus is currently running.

        Returns:
            bool: True if the messagebus is running, False otherwise.
        """

    @abc.abstractmethod
    def is_closed(self) -> bool:
        """
        Checks if the messagebus is closed.

        Returns:
            bool: True if the messagebus is closed, False otherwise.
        """

    @abc.abstractmethod
    def include_collection(self, collection: IHandlersCollection):
        """
        Include a collection of handlers.

        Args:
            collection: An instance of the IHandlersCollection class that contains a collection of handlers.
        """

    @abc.abstractmethod
    def get_context(self) -> Context:
        """
        Get the context of the current instance.

        Returns:
            Context: The context of the current environment.
        """


class IMessagebusPolicy(abc.ABC):
    """
    This class represents an abstract policy interface for managing messagebus in a system.

    Methods:
        get_messagebus: Get the messagebus for the current context.
        set_messagebus: Set the messagebus for the current context.
        new_messagebus: Create and return a new messagebus object, according to this policy's rules.
    """

    @abc.abstractmethod
    def get_messagebus(self) -> IMessagebus:
        """Get or create a messagebus for current context.

        Returns:
            IMessagebus: The messagebus for current context
        """

    @abc.abstractmethod
    def set_messagebus(self, messagebus: IMessagebus | None):
        """
        Set the messagebus for the current context.

        Args:
            messagebus (IMessagebus|None): The messagebus for set to current context.
        """

    @abc.abstractmethod
    def new_messagebus(self) -> IMessagebus:
        """
        Create and return a new messagebus object according to this
        policy's rules. If there's need to set this messagebus as the messagebus for
        the current context, `set_messagebus` must be called explicitly.

        Returns:
            IMessagebus: The new messagebus instance
        """


_ReferenceType = TypeVar("_ReferenceType")


class IEntity(abc.ABC, Generic[_ReferenceType]):
    """
    Abstract base class for all entity classes.

    Attributes:
        __reference__ (_ReferenceType): Property that represents the reference of the entity.
    """

    @property
    @abc.abstractmethod
    def __reference__(self) -> _ReferenceType:
        """
        Property that represents the reference of the entity.

        Returns:
            entity reference
        """


Version = NewType("Version", int)


class IRootEntityMeta(abc.ABCMeta):
    @property
    @abc.abstractmethod
    def __domain_name__(cls) -> DomainName:
        """
        Property that represents the reference of the root entity.

        Returns:
            entity domain
        """


class IRootEntity(
    IEntity[_ReferenceType], abc.ABC, Generic[_ReferenceType], metaclass=IRootEntityMeta
):
    """
    This class represents an abstract root entity in a domain-driven design architecture.
    It is a base class for all root entities in the domain.

    Attributes:
        __reference__ (_ReferenceType): Property that represents the reference of the root entity.
        __version__ (Version): Property that represents the version of the root entity.
        __domain_name__ (DomainName): Property that represents the domain name of the root entity.

    Methods:
        collect_events: Collects domain events from the root entity and removes them after the call.
    """

    @property
    @abc.abstractmethod
    def __version__(self) -> Version:
        """
        Property that represents the version of the root entity.

        Returns:
            Version: root entity version
        """

    @property
    @abc.abstractmethod
    def __domain_name__(self) -> DomainName:
        """
        Represents the domain name of the root entity.

        Returns:
            DomainName: entity domain
        """

    @abc.abstractmethod
    def collect_events(self) -> Iterable[AbstractEvent]:
        """
        Collects domain events from the root entity and removes them after the call.

        Returns:
            Iterable[IMessage]: domain events
        """
