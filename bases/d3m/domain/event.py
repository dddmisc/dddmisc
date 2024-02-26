from typing import TypeVar

from d3m.core import (
    MessageType,
    AbstractEvent,
    DomainName,
)
from pydantic._internal._model_construction import ModelMetaclass  # noqa

from d3m.core.abstractions import AbstractEventMeta
from d3m.domain.collection import DomainObjectsCollection
from d3m.domain.bases import BaseDomainMessageMeta, BaseDomainMessage

_T = TypeVar("_T", bound="DomainEvent")


class _DomainEventMeta(BaseDomainMessageMeta, AbstractEventMeta):
    def __init__(cls, name, bases, namespace, *, domain: str | None = None):
        super().__init__(name, bases, namespace, domain=domain)
        if domain is None and cls.__module__ != __name__:
            try:
                _ = cls.__domain_name__
            except AttributeError:
                raise ValueError(
                    f"required set domain name for event '{cls.__module__}.{cls.__name__}'"
                )


class DomainEvent(BaseDomainMessage, AbstractEvent, metaclass=_DomainEventMeta):
    """
    Class representing a base domain event.

    Attributes:
        __domain_name__ (DomainName): The domain name associated with the current event.
        __message_name__ (MessageName): The message name associated with the current event.
        __payload__ (dict): The payload of the event.
        __type__ (MessageType): Always return `MessageType.EVENT`
        __reference__ (UUID): The unique identifier for the event.
        __timestamp__ (datetime.datetime): The timestamp of when the event was created.

    Methods:
        to_dict: Returns a dictionary representation payload of the event.
        to_json: Returns a JSON string representation payload of the event.
        load: class method returns new instance of the domain event

    Examples:
        **Create event class:**
        >>> from d3m.domain import DomainEvent
        >>> class PersonCreated(DomainEvent, domain='person'):
        ...     reference: UUID
        ...     name: str
        ...     surname: str
        >>> assert PersonCreated.__domain_name__ == DomainName('person')
        >>> assert PersonCreated.__message_name__ == MessageName('PersonCreated')
        >>> assert PersonCreated.__type__ == MessageType.EVENT

        **Create event instance**
        >>> event = PersonCreated(
        ...             reference=person.__reference__
        ...             name=person.name,
        ...             surname=person.surname
        ...         )
        >>> assert event.__domain_name__ == DomainName('person')
        >>> assert event.__message_name__ == MessageName('PersonCreated')
        >>> assert isinstance(event.__reference__, UUID)
        >>> assert isinstance(event.__timestamp__, datetime)
        >>> assert event.__payload__ == dict(reference=person.__reference__, name=person.name, surname=person.surname)
        >>> assert event.reference == person.__reference__
        >>> assert event.name == person.name
        >>> assert event.surname == person.surname

        **Load event from dict**
        >>> payload = dict(
        ...               reference=person.__reference__
        ...               name=person.name,
        ...               surname=person.surname
        ...           )
        >>> event = PersonCreated.load(payload, reference=uuid4(), timestamp=datetime.now())
        >>> assert event.__domain_name__ == DomainName('person')
        >>> assert event.__message_name__ == MessageName('PersonCreated')
        >>> assert isinstance(event.__reference__, UUID)
        >>> assert isinstance(event.__timestamp__, datetime)
        >>> assert event.__payload__ == payload

        **Load event from json-string**
        >>> payload = '{"reference":"00000000-0000-0000-0000-000000000001","name":"John","surname":"Black"}'
        >>> event = PersonCreated.load(payload, reference=uuid4(), timestamp=datetime.now())
        >>> assert event.__domain_name__ == DomainName('person')
        >>> assert event.__message_name__ == MessageName('PersonCreated')
        >>> assert isinstance(event.__reference__, UUID)
        >>> assert isinstance(event.__timestamp__, datetime)
        >>> assert event.name == 'John'
        >>> assert event.surname =='Black'
        >>> assert event.reference == UUID(int=1)

        **Event's payload to json serializeble dict**
        >>> assert event.to_dict() == {'reference': '00000000-0000-0000-0000-000000000001', 'name': 'John', 'surname': 'Black'}

        **Event's payload to json**
        >>> assert event.to_dict() == '{"reference":"00000000-0000-0000-0000-000000000001","name":"John","surname":"Black"}'
    """


def get_event_class(
    domain: DomainName | str, name: DomainName | str
) -> AbstractEventMeta:
    """
    Return registered event class by domain and name

    Attributes:
        domain (DomainName | str): The domain of the event.
        name (MessageName | str): The name of the event.

    Returns:
        The class of the event.


    Examples:
        >>> from d3m.domain import DomainEvent
        >>> class PersonCreated(DomainEvent, domain='person')
        ...     name: str
        ...     surname: str
        >>> event_class = get_event_class('person', 'PersonCreated')
        >>> assert event_class is PersonCreated
    """
    return DomainObjectsCollection().get_domain_object(MessageType.EVENT, domain, name)
