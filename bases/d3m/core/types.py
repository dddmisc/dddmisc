import json
import re
import datetime as dt
import warnings
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Iterable, Callable, Any


class MessagebusEvents(str, Enum):
    """
    This class representing different events that can occur in a Messagebus.

    Attributes:
        BEFORE_RUN (str): Event triggered before running the Messagebus.
        AFTER_RUN (str): Event triggered after running the Messagebus.
        BEFORE_STOP (str): Event triggered before stopping the Messagebus.
        AFTER_STOP (str): Event triggered after stopping the Messagebus.
        BEFORE_CLOSE (str): Event triggered before closing the Messagebus.
        AFTER_CLOSE (str): Event triggered after closing the Messagebus.
    """

    BEFORE_RUN = "BEFORE_RUN"
    AFTER_RUN = "AFTER_RUN"
    BEFORE_STOP = "BEFORE_STOP"
    AFTER_STOP = "AFTER_STOP"
    BEFORE_CLOSE = "BEFORE_CLOSE"
    AFTER_CLOSE = "AFTER_CLOSE"


class MessageType(str, Enum):
    """

    This class represents different types of message.

    Attributes:
        COMMAND (MessageType): The command type.
        EVENT (MessageType): The event type.

    """

    COMMAND = "COMMAND"
    EVENT = "EVENT"


class DomainName(str):
    """
    DomainName class represents a domain name string. It extends the built-in `str` class and adds validation for domain name format.

    Attributes:
        part_of (Optional[DomainName]): Gets the parent DomainName of a sub-domain.

    Raises:
        ValueError: If the domain name contains disallowed symbols.

    """

    def __new__(cls, value):
        if isinstance(value, cls):
            return value
        return super().__new__(cls, value)

    def __init__(self, value: str):
        items = value.rsplit(".", maxsplit=1)
        is_subdomain = len(items) != 1
        self._validate(items[1] if is_subdomain else items[0])
        self._part_of: DomainName | None = (
            DomainName(items[0]) if is_subdomain else None
        )

    @property
    def part_of(self):
        """
        Gets the parent DomainName of a sub-domain.

        Returns:
            (Optional[DomainName]): The parent DomainName instance or None if it's not a sub-domain.
        """
        return self._part_of

    def _validate(self, value: str):
        if not re.search(r"^([a-z]|[a-z0-9]-)+$", value):
            raise ValueError(
                f'DomainName "{self}" has not allowed symbols in section "{value}"'
            )

    def __repr__(self):
        return f"DomainName('{self}')"


class MessageName(str):
    """
    Class representing a message name.  It extends the built-in `str` class and adds validation for message name format.

    Raises:
        ValueError: If the message name contains disallowed symbols.
    """

    def __new__(cls, value):
        if isinstance(value, cls):
            return value
        cls._validate(value)
        return super().__new__(cls, value)

    @staticmethod
    def _validate(value: str):
        if not re.search("^[A-Z][A-Za-z0-9]+$", value):
            raise ValueError(f"MsgName {str(value)!r} has not allowed symbols")

    def __repr__(self):
        return f"MessageName('{self}')"


class JsonDict(dict):
    JSON_CONVERTER = json
    CONVERTABLE_TYPES = (bool, int, float, str, type(None))
    CONVERTERS: Mapping[type, Callable[[Any], Any]] = MappingProxyType(
        {
            dt.date: lambda x: x.isoformat(),
            Enum: lambda x: x.value,
        }
    )
    ITERABLE_CONVERTERS: Mapping[
        type[Iterable], Callable[[Iterable], Iterable]
    ] = MappingProxyType({tuple: tuple})
    DEFAULT_ITERABLE_CONVERTER: Callable[[Iterable], Iterable] = list

    def __init__(self, __obj: Mapping | None = None, /, **kwargs):
        result = self._parse_object(kwargs if __obj is None else __obj)
        super().__init__(result)

    def _parse_object(self, obj: Mapping):
        result = {}
        markers = {id(obj): obj}
        for key, value in obj.items():
            result[key] = self._parse_value(value, markers)
        return result

    def _parse_mapping(self, obj: Mapping, markers: dict):
        marker_id = id(obj)
        if marker_id in markers:
            raise ValueError("Circular reference detected")
        markers[id(obj)] = obj

        result = self.__class__()
        for key, value in obj.items():
            dict.__setitem__(result, key, self._parse_value(value, markers))
        return result

    def _parse_iterable(self, values: Iterable, markers) -> Iterable:
        marker_id = id(values)
        if marker_id in markers:
            raise ValueError("Circular reference detected")
        markers[id(values)] = values

        result = [self._parse_value(value, markers) for value in values]
        return self._get_iterable_type(values)(result)

    def _get_iterable_type(self, values) -> Callable[[Iterable], Iterable]:
        for type_, converter in self.ITERABLE_CONVERTERS.items():
            if isinstance(values, type_):
                return converter
        if self.DEFAULT_ITERABLE_CONVERTER is None:
            return type(values)  # pragma: no cover
        return self.DEFAULT_ITERABLE_CONVERTER

    def _parse_value(self, value, markers: dict):
        if isinstance(value, self.CONVERTABLE_TYPES):
            return value
        elif isinstance(value, tuple(self.CONVERTERS.keys())):
            return self._parse_other(value)
        elif isinstance(value, Mapping):
            return self._parse_mapping(value, markers)
        elif isinstance(value, Iterable):
            return self._parse_iterable(value, markers)
        else:
            return str(value)

    def _parse_other(self, value):
        for type_, converter in self.CONVERTERS.items():
            if isinstance(value, type_):
                return converter(value)

    def __repr__(self):
        return super().__repr__()

    def __str__(self):
        return self.JSON_CONVERTER.dumps(self)

    def __setitem__(self, key, value):
        value = self._parse_value(value, {id(self): self})
        super().__setitem__(key, value)

    def setdefault(self, __key, __default=None):
        if __key not in self:
            self[__key] = __default
        return self[__key]

    def update(self, __m=None, **kwargs):
        if isinstance(__m, dict):
            __m = __m.items()
        if __m is not None:
            for key, value in __m:
                self[key] = value

        for key, value in kwargs.items():
            self[key] = value


class FrozenJsonDict(JsonDict):
    DEFAULT_ITERABLE_CONVERTER = tuple

    def __init__(self, __obj: Mapping | None = None, /, **kwargs):
        super().__init__(__obj, **kwargs)
        self.__hash = hash(str(self))

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __str__(self):
        if not hasattr(self, "__json"):
            setattr(self, "__json", super().__str__())
        return getattr(self, "__json")

    def update(self, __m=None, **kwargs):
        warnings.warn("Method not implemented", DeprecationWarning, stacklevel=2)
        raise NotImplementedError()

    def pop(self, __key):
        warnings.warn("Method not implemented", DeprecationWarning, stacklevel=2)
        raise NotImplementedError()

    def popitem(self):
        warnings.warn("Method not implemented", DeprecationWarning, stacklevel=2)
        raise NotImplementedError()

    def setdefault(self, __key, __default=None):
        warnings.warn("Method not implemented", DeprecationWarning, stacklevel=2)
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def clear(self):
        warnings.warn("Method not implemented", DeprecationWarning, stacklevel=2)
        raise NotImplementedError()

    def __hash__(self):
        return self.__hash
