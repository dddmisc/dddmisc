import abc
import datetime as dt
from contextlib import suppress
from typing import Mapping, TypeVar, no_type_check, Type
from uuid import UUID, uuid4

from pydantic import BaseModel, PrivateAttr, ConfigDict
from pydantic._internal._model_construction import ModelMetaclass  # noqa

from d3m.core import IMessageMeta, MessageName, DomainName, IMessage
from .collection import DomainObjectsCollection


def get_domain_name(
    cls,
    bases: tuple[type[IMessageMeta]],
    domain: DomainName | str | None = None,
) -> DomainName | None:
    domains: set[DomainName] = set()  # type: ignore
    for base in bases:
        with suppress(AttributeError):
            if base.__domain_name__ is not None:
                domains.add(base.__domain_name__)  # type: ignore

    if len(domains) > 1:
        raise RuntimeError("Not allowed multiple inheritance domain")

    if len(domains) == 1 and domain is not None and domain != cls.__domain_name__:
        raise RuntimeError(
            f"not allowed replace domain name in child class: {cls.__module__}.{cls.__name__}"
        )

    if len(domains) == 0 and domain is not None:
        return DomainName(domain)

    if len(domains) == 1 and (domain is None or domain == cls.__domain_name__):
        return cls.__domain_name__  # type: ignore

    return None


_T = TypeVar("_T", bound="BaseDomainMessage")


class BaseDomainMessageMeta(IMessageMeta, ModelMetaclass, abc.ABCMeta):
    __domain_name: DomainName

    @no_type_check
    def __new__(mcs, name, bases, namespace, domain: str | None = None):
        cls = super().__new__(mcs, name, bases, namespace)
        domain = get_domain_name(cls, bases, domain)
        if domain is not None:
            cls.__domain_name = domain
        cls.model_config["frozen"] = True
        return cls

    def __init__(cls, name, bases, namespace, *, domain: str | None = None):
        super().__init__(name, bases, namespace, domain=domain)

        cls.__message_name = MessageName(name)

        with suppress(AttributeError):
            DomainObjectsCollection().register(
                category=cls.__type__,
                domain=cls.__domain_name__,
                name=cls.__message_name__,
                klass=cls,
            )

    @property
    def __domain_name__(cls) -> DomainName:
        return cls.__domain_name

    @property
    def __message_name__(cls) -> MessageName:
        return cls.__message_name

    @property
    def __payload_model__(cls) -> Type[BaseModel]:
        return cls  # type: ignore[return-value]

    @no_type_check
    def load(
        cls: type[_T],
        payload: Mapping | str | bytes,
        reference: UUID | None = None,
        timestamp: dt.datetime | None = None,
    ) -> _T:
        obj = cls.model_validate(payload)
        obj.__pydantic_private__["_BaseDomainMessage__reference"] = (
            reference or obj.__reference__
        )
        obj.__pydantic_private__["_BaseDomainMessage__timestamp"] = (
            timestamp or obj.__timestamp__
        )
        return obj


class BaseDomainMessage(IMessage, BaseModel, abc.ABC, metaclass=BaseDomainMessageMeta):
    """
    Class representing a base domain message.

    Attributes:
        __domain_name__ (DomainName): The domain name associated with the current class.
        __message_name__ (MessageName): The message name associated with the current class.
        __payload__ (dict): The payload of the message.
        __reference__ (UUID): The unique identifier for the message.
        __timestamp__ (datetime.datetime): The timestamp of when the message was created.

    Methods:
        to_dict: Returns a dictionary representation of the message.
        to_json: Returns a JSON string representation of the message.

    """

    __reference: UUID = PrivateAttr(default_factory=uuid4)
    __timestamp: dt.datetime = PrivateAttr(
        default_factory=lambda: dt.datetime.now(dt.timezone.utc)
    )

    model_config = ConfigDict(frozen=True)

    @property
    def __domain_name__(self) -> DomainName:
        return self.__class__.__domain_name__  # type: ignore

    @property
    def __message_name__(self) -> MessageName:
        return self.__class__.__message_name__  # type: ignore

    @property
    def __reference__(self) -> UUID:
        return self.__reference

    @property
    def __timestamp__(self) -> dt.datetime:
        return self.__timestamp

    @property
    def __payload__(self) -> dict:
        return self.model_dump()

    def to_dict(self) -> dict:
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        return self.model_dump_json()
