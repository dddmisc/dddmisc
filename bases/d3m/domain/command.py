from d3m.core import (
    MessageType,
    AbstractCommand,
)

from pydantic._internal._model_construction import ModelMetaclass  # noqa

from d3m.core.abstractions import AbstractCommandMeta
from .collection import DomainObjectsCollection
from .bases import BaseDomainMessageMeta, BaseDomainMessage


class _DomainCommandMeta(BaseDomainMessageMeta, AbstractCommandMeta):
    def __init__(cls, name, bases, namespace, *, domain: str | None = None):
        super().__init__(name, bases, namespace, domain=domain)
        if domain is None and cls.__module__ != __name__:
            try:
                _ = cls.__domain_name__
            except AttributeError:
                raise ValueError(
                    f"required set domain name for command '{cls.__module__}.{cls.__name__}'"
                )


class DomainCommand(BaseDomainMessage, AbstractCommand, metaclass=_DomainCommandMeta):
    """
    Class representing a base domain command.

    Attributes:
        __domain_name__ (DomainName): The domain name associated with the current command.
        __message_name__ (MessageName): The message name associated with the current command.
        __payload__ (dict): The payload of the command.
        __payload_model__ (Type[BaseModel]): The payload model associated with the current command.
        __type__ (MessageType): Always return `MessageType.COMMAND`
        __reference__ (UUID): The unique identifier for the command.
        __timestamp__ (datetime.datetime): The timestamp of when the command was created.

    Methods:
        to_dict: Returns a dictionary representation payload of the command.
        to_json: Returns a JSON string representation payload of the command.
        load: class method returns new instance of the domain command

    Examples:
        **Create command class:**
        >>> from d3m.domain import DomainCommand
        >>> class CreatePerson(DomainCommand, domain='person'):
        ...     name: str
        ...     surname: str
        >>> assert CreatePerson.__domain_name__ == DomainName('person')
        >>> assert CreatePerson.__message_name__ == MessageName('CreatePerson')
        >>> assert CreatePerson.__type__ == MessageType.COMMAND

        **Create command instance**
        >>> command = CreatePerson(
        ...               name='John',
        ...               surname='Black'
        ...           )
        >>> assert command.__domain_name__ == DomainName('person')
        >>> assert command.__message_name__ == MessageName('CreatePerson')
        >>> assert isinstance(command.__reference__, UUID)
        >>> assert isinstance(command.__timestamp__, datetime)
        >>> assert command.__payload__ == dict(name='John', surname='Black')
        >>> assert command.name == 'John'
        >>> assert command.surname =='Black'

        **Load command from dict**
        >>> payload = dict(name='John', surname='Black')
        >>> reference = uuid4()
        >>> timestamp = datetime.now(timezone.utc)
        >>> command = CreatePerson.load(payload, reference=reference, timestamp=timestamp)
        >>> assert command.__domain_name__ == DomainName('person')
        >>> assert command.__message_name__ == MessageName('CreatePerson')
        >>> assert command.__reference__ == reference
        >>> assert command.__timestamp__ == timestamp
        >>> assert command.__payload__ == payload

        **Load command from json-string**
        >>> payload = '{"name":"John","surname":"Black"}'
        >>> command = CreatePerson.load(payload, reference=uuid4(), timestamp=datetime.now())
        >>> assert command.__domain_name__ == DomainName('person')
        >>> assert command.__message_name__ == MessageName('CreatePerson')
        >>> assert isinstance(command.__reference__, UUID)
        >>> assert isinstance(command.__timestamp__, datetime)
        >>> assert command.name == 'John'
        >>> assert command.surname =='Black'

        **Command's payload to json serializeble dict**
        >>> assert command.to_dict() == {'name': 'John', 'surname': 'Black'}

        **Command's payload to json**
        >>> assert command.to_dict() == '{"name":"John","surname":"Black"}'
    """


def get_command_class(domain: str, name: str) -> AbstractCommandMeta:
    """
    Return registered command class by domain and name

    Attributes:
        domain (DomainName | str): The domain of the event.
        name (MessageName | str): The name of the event.

    Returns:
        The class of the command.

    Examples:
        >>> from d3m.domain import DomainCommand
        >>> class CreatePerson(DomainCommand, domain='person')
        ...     name: str
        ...     surname: str
        >>> command_class = get_command_class('person', 'CreatePerson')
        >>> assert command_class is CreatePerson
    """
    return DomainObjectsCollection().get_domain_object(
        MessageType.COMMAND, domain, name
    )
