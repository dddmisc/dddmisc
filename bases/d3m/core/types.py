import re
from enum import Enum


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
