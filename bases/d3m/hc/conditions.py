import abc

from d3m.core import AbstractEvent


class ICondition(abc.ABC):
    """
    Base interface for conditions classes used for filter events

    Methods:
        check: Checks the event against the specified condition

    Returns:
        bool: True when consistent, False when inconsistent
    """

    @abc.abstractmethod
    def check(self, event: AbstractEvent) -> bool:
        """
        Checks the event against the specified condition

        Args:
            event (AbstractEvent): event

        Returns:
             `True` when consistent, `False` when inconsistent
        """


class _NoneCondition(ICondition):
    def check(self, event: AbstractEvent) -> bool:
        return True


none_condition = _NoneCondition()


class HasAttrs(ICondition):
    """
    A condition that checks if an event has all the specified attributes.

    Args:
        *attrs: A variable number of attribute names as strings.

    Methods:
        check: Checks if an event has all the specified attributes.

    Examples:
        >>> has_attrs = HasAttrs('attribute1', 'attribute2')
        >>> event = MyEvent({'attribute1': 'value1', 'attribute2': 'value2', 'attribute3': 'value3'})
        >>> has_attrs.check(event)
        True
    """

    def __init__(self, *attrs: str):
        self._attrs = set(attrs)

    def check(self, event: AbstractEvent) -> bool:
        return self._attrs.issubset(set(event.__payload__.keys()))


class And(ICondition):
    """
    "And" condition.

    A class representing a logical AND condition.

    Args:
        *conditions: Initializes an And instance with the given conditions.

    Methods:
        check: Checks if an event against all the specified conditions
    """

    def __init__(self, *conditions: ICondition):
        for c in conditions:
            if not isinstance(c, ICondition):
                raise TypeError(f"{c!r} required be is instance of {ICondition!r}")
        self._conditions = conditions

    def check(self, event: AbstractEvent) -> bool:
        return all((condition.check(event) for condition in self._conditions))


class Or(ICondition):
    """
    "Or" condition.

    A class representing a logical OR condition.

    Args:
        *conditions: Initializes an Or instance with the given conditions.

    Methods:
        check: Checks if an event against any the specified conditions
    """

    def __init__(self, *conditions: ICondition):
        for c in conditions:
            if not isinstance(c, ICondition):
                raise TypeError(f"{c!r} required be is instance of {ICondition!r}")
        self._conditions = conditions

    def check(self, event: AbstractEvent) -> bool:
        return any((condition.check(event) for condition in self._conditions))


class Not(ICondition):
    """
    This class represents a logical NOT condition that can be used to negate the result of another condition.

    Args:
        condition (ICondition): The condition to be negated.

    Methods:
        check: Checks if an event not against the specified condition

    """

    def __init__(self, condition: ICondition):
        if not isinstance(condition, ICondition):
            raise TypeError(f"{condition!r} required be is instance of {ICondition!r}")
        self._condition = condition

    def check(self, event: AbstractEvent) -> bool:
        return not self._condition.check(event)


class Equal(ICondition):
    """
    Class representing an 'Equal' condition.

    This class implements the 'ICondition' interface and provides a way to check if certain attributes in an event's payload are equal to specified values.

    Args:
        **attrs: A containing the attributes and values to check.

    Methods:
        check: Checks if the attributes in the event's payload are equal to the specified values.
    """

    def __init__(self, **attrs):
        self._attrs = attrs

    def check(self, event: AbstractEvent) -> bool:
        return all(
            (
                key in event.__payload__ and event.__payload__[key] == value
                for key, value in self._attrs.items()
            )
        )
