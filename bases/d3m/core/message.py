import datetime as dt
from copy import deepcopy
from uuid import UUID, uuid4

from pydantic_core import to_jsonable_python, to_json

from . import IMessage, DomainName, MessageName, MessageType
from .helpers import parse_full_message_name


class UniversalMessage(IMessage):
    """
    A universal class for create domain's messages.

    Args:
        full_message_name: The full name of the message, including the domain and name.
        message_type: The type of the message, either as an instance of MessageType or a string representing the type.
        payload: The payload of the message as a dictionary.
        reference: Optional. A UUID representing the reference of the message. If not provided, a new UUID will be generated.
        timestamp: Optional. The timestamp of the message. If not provided, the current time in UTC will be used.

    Attributes:
        __domain_name__ (DomainName): The domain name of the message.
        __message_name__ (MessageName): The name of the message.
        __type__ (MessageType): The type of the message.
        __reference__ (UUID): The reference of the message.
        __timestamp__ (datetime): The timestamp of the message.
        __payload__ (dict): The payload of the message.

    Methods:
        to_dict: Returns the payload of the message as a dictionary.
        to_json: Returns the payload of the message as a JSON string.

    Examples:
        **Create command**
        >>> command = UniversalMessage('custom-domain.subdomain.CommandName', 'command', {'arg1': 123})
        >>> command
        UniversalMessage("custom-domain.subdomain.CommandName", <MessageType.COMMAND: 'COMMAND'>, {'arg1': 123}, UUID('a19019fc-5e55-4b4c-9620-4e1cbf01e73a'), datetime.datetime(2024, 2, 14, 11, 58, 5, 244542, tzinfo=datetime.timezone.utc)))
        >>> assert isinstance(event, AbstractCommand)
        >>> assert command.__domain_name__ == 'custom-domain.subdomain'
        >>> assert command.__message_name__ == 'CommandName'
        >>> assert command.__type__ == MessageType.COMMAND
        >>> assert command.__payload__ == dict(arg1=123)
        >>> assert isinstance(command.__reference__, UUID)
        >>> assert isinstance(command.__timestamp__, datetime)
        >>> command.to_json()
        '{"arg1": 123}'
        >>> command.to_dict()
        {'arg1': 123}

        **Create event**
        >>> event = UniversalMessage('custom-domain.subdomain.EventName', 'event', {'arg': "abc"})
        >>> event
        UniversalMessage("custom-domain.subdomain.EventName", <MessageType.EVENT: 'EVENT'>, {'arg1': 123}, UUID('a19019fc-5e55-4b4c-9620-4e1cbf01e73a'), datetime.datetime(2024, 2, 14, 11, 58, 5, 244542, tzinfo=datetime.timezone.utc)))
        >>> assert isinstance(event, AbstractEvent)
        >>> assert event.__domain_name__ == 'custom-domain.subdomain'
        >>> assert event.__message_name__ == 'EventName'
        >>> assert event.__type__ == MessageType.EVENT
        >>> assert event.__payload__ == dict(arg1=123)
        >>> assert isinstance(event.__reference__, UUID)
        >>> assert isinstance(event.__timestamp__, datetime)
        >>> event.to_json()
        '{"arg": "abc"}'
        >>> event.to_dict()
        {'arg': 'abc'}
    """

    def __init__(
        self,
        full_message_name: str,
        message_type: MessageType | str,
        payload: dict,
        reference: UUID | None = None,
        timestamp: dt.datetime | None = None,
    ):
        self._domain, self._name = parse_full_message_name(full_message_name)
        self._type = (
            MessageType(message_type.upper())
            if isinstance(message_type, str)
            else message_type
        )
        self._payload = deepcopy(payload)

        self._reference = reference or uuid4()
        self._timestamp = timestamp or dt.datetime.now(dt.timezone.utc)
        if self._timestamp.tzinfo is None:
            self._timestamp = dt.datetime.combine(
                self._timestamp.date(), self._timestamp.time(), dt.timezone.utc
            )

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f'"{self._domain}.{self._name}", '
            f"{self._type!r}, "
            f"{self._payload!r}, "
            f"{self._reference!r}, "
            f"{self._timestamp!r})"
            f")"
        )

    @property
    def __domain_name__(self) -> DomainName:
        return self._domain

    @property
    def __message_name__(self) -> MessageName:
        return self._name

    @property
    def __type__(self) -> MessageType:
        return self._type

    @property
    def __reference__(self) -> UUID:
        return self._reference

    @property
    def __timestamp__(self) -> dt.datetime:
        return self._timestamp

    @property
    def __payload__(self) -> dict:
        return self._payload

    def to_dict(self) -> dict:
        return to_jsonable_python(self._payload, serialize_unknown=True)

    def to_json(self) -> str:
        return to_json(self._payload, serialize_unknown=True).decode()
