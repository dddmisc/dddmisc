from uuid import UUID, uuid4

from pydantic import BaseModel
from typing import Generic, Callable, Iterable, Any, NewType
from pydantic._internal._model_construction import ModelMetaclass  # noqa

from d3m.core import (
    MessageName,
    IEntity,
    IRootEntity,
    AbstractEvent,
    DomainName,
)
from d3m.core.abstractions import Version, IRootEntityMeta, _ReferenceType
from . import get_event_class
from .bases import get_domain_name


class _EntityMeta(ModelMetaclass):
    _reference_factory: Callable[[], Any] | None = None
    _reference_type: type | None = None

    def __init__(cls, name, bases, namespace, *, reference_factory=None, **kwargs):
        super().__init__(name, bases, namespace, **kwargs)
        generic = cls.__pydantic_generic_metadata__.get("args", ())  # type: ignore[attr-defined]
        if not generic:
            cls._reference_type = cls._reference_type or UUID
        elif type(generic[0]) is NewType:
            cls._reference_type = generic[0].__supertype__  # type: ignore[assignment]
        else:
            cls._reference_type = generic[0]

        if reference_factory is not None:
            cls._reference_factory = staticmethod(reference_factory)
        elif cls._reference_type is UUID:
            cls._reference_factory = staticmethod(cls._reference_factory or uuid4)

    def __call__(cls, *, __reference__=None, **date):
        obj = super().__call__(**date)
        if __reference__ is None and obj._reference_factory is not None:
            __reference__ = obj._reference_factory()
        if not isinstance(__reference__, obj._reference_type):
            raise TypeError(f"Invalid reference type. Expected {obj._reference_type!r}")
        obj.__pydantic_private__["_Entity__reference"] = __reference__
        return obj


class Entity(IEntity, BaseModel, Generic[_ReferenceType], metaclass=_EntityMeta):
    """
    Class Entity

    A base class representing an entity.

    Attributes:
        __reference__ (_ReferenceType): The reference of the entity.

    Examples:
        **Create an entity class:**
        >>> class Person(Entity):
        ...     name: str
        ...     surname: str
        ...
        >>> person = Person(name="John", surname="Doe")
        >>> assert isinstance(person.__reference__, UUID)

        by default reference type is `UUID` and default factory is `uuid.uuid4()`

        **Set custom reference value:**
        >>> import uuid
        >>> person = Person(name="John", surname="Doe", __reference__=uuid.UUID(int=1))
        >>> assert person.__reference__ == uuid.UUID(int=1)

        **Set custom reference type:**
        >>> PersonId = int
        >>> class Person(Entity[PersonId]):
        ...     name: str
        ...     surname: str
        >>> # when set custom reference type, the default factory is not set
        >>> person = Person(name="John", surname="Doe", __reference__=1)
        >>> assert person.__reference__ == 1

        **Set default factory:**
        >>> import random
        >>> class Person(Entity[PersonId], default_factory=random.randrange(1, 1000000)):
        ...     name: str
        ...     surname: str
        >>> person = Person(name="John", surname="Doe")
        >>> assert isinstance(person.__reference__, int)
        >>> assert 1 <= person.__reference__ < 1000000

    """

    __reference: _ReferenceType

    def __init_subclass__(cls, *, reference_factory=None, **kwargs):
        super().__init_subclass__(**kwargs)

    @property
    def __reference__(self) -> _ReferenceType:
        """
        Returns the reference of the entity.

        Returns:
            _ReferenceType: The reference of the entity.
        """
        return self.__reference

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.__reference__ == other.__reference__
        )

    def __hash__(self):
        return hash(self.__reference__)


class _RootEntityMeta(_EntityMeta, IRootEntityMeta, ModelMetaclass):
    def __init__(cls, name, bases, namespace, *, domain: str | None = None, **kwargs):
        super().__init__(name, bases, namespace, **kwargs)
        domain = get_domain_name(cls, bases, domain)
        if domain is not None:
            cls.__domain_name = domain
        elif domain is None and cls.__module__ != __name__:
            raise ValueError(
                f"required set domain name for root entity '{cls.__module__}.{cls.__name__}'"
            )

    def __call__(cls, __version__=Version(1), **data):
        obj = super().__call__(**data)
        obj.__pydantic_private__["_RootEntity__version"] = __version__
        return obj

    @property
    def __domain_name__(cls) -> DomainName:
        return cls.__domain_name


class RootEntity(
    Entity,
    IRootEntity,
    Generic[_ReferenceType],
    metaclass=_RootEntityMeta,
):
    """
    RootEntity

    A root entity is a base class for domain entities that serves as the root of an aggregate.
     It provides common functionality for managing events and versioning.

    Attributes:
        __reference__ (_ReferenceType): the reference of the root entity.
        __domain_name__ (DomainName): the domain name associated with the root entity
        __version__ (Version): The current version of the root entity.

    Methods:
        create_event: Create a domain event for this entity.
        collect_events: Collect all the events that have been created for this entity.

    Examples:
        **Set domain of root entity:**
        >>> class Person(RootEntity, domain='person'):
        ...     name: str
        ...     surname: str
        >>> assert Person.__domain_name__ == DomainName('person')
        >>> person = Person(name="John", surname="Doe")
        >>> assert person.__domain_name__ == DomainName('person')
        >>> assert isinstance(person.__reference__, UUID)
        >>> assert person.__version__ == 1

        **Set custom version of root entity:**
        >>> person = Person(name="John", surname="Doe", __version__=2)
        >>> assert person.__version__ == 2

        **Create and collect domain events for this entity:**
        >>> events = list(person.collect_events())
        >>> assert len(events) == 0
        >>> person.create_event('PersonCreated',
        ...                     reference=person.__reference__,
        ...                     name=person.name,
        ...                     surname=person.surname)
        >>> events = list(person.collect_events())
        >>> assert len(events) == 1
        >>> event = events[0]
        >>> assert isinstance(event, AbstractEvent)
        >>> assert event.__domain_name__ == DomainName('person')
        >>> assert event.__message_name__ == MessageName('PersonCreated')
        >>> events = list(person.collect_events())
        >>> assert len(events) == 0

    """

    __version: Version = Version(1)

    def __init_subclass__(cls, *, domain: str | None = None, **kwargs):
        super().__init_subclass__()

    def __init__(self, **data):
        super().__init__(**data)
        self._events: set[AbstractEvent] = set()

    @property
    def __domain_name__(self) -> DomainName:
        """
        Get the domain name associated with the current class.

        Returns:
            DomainName: The domain name associated with the class.

        """
        return self.__class__.__domain_name__  # type: ignore

    @property
    def __version__(self) -> Version:
        """
        Get the current version of the root entity.

        Returns:
             Version: The current version of the root entity.
        """
        return self.__version

    def create_event(self, __name: MessageName | str, /, **payload):
        """
        Create a domain event for this root entity.

        Attributes:
            __name (MessageName|str): The name of the event. Event class required be declared for domain of the root entity.
            **payload: Additional keyword arguments to be passed as payload data for creating the event.

        """
        event_class = get_event_class(self.__domain_name__, __name)
        event = event_class.load(payload)
        self._events.add(event)

    def collect_events(self) -> Iterable[AbstractEvent]:
        """
        Collects and returns events from this root entity.

        Returns:
            An iterable of `AbstractEvent` objects.
        """
        while self._events:
            yield self._events.pop()


def increment_version(entity: RootEntity):
    """
    Increments the version of the root entity

    Attributes:
        entity (RootEntity): The root entity object whose version needs to be incremented.

    """
    if isinstance(entity.__pydantic_private__, dict):
        version = entity.__pydantic_private__.get("_RootEntity__version")
        if version:
            version += 1
        entity.__pydantic_private__["_RootEntity__version"] = version
