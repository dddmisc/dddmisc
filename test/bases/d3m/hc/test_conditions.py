import pytest
from d3m.hc import And, Equal, HasAttrs, Not, Or
from d3m.hc.conditions import none_condition
from d3m.core import UniversalMessage


@pytest.mark.parametrize(
    "attrs, result",
    [
        (("key1",), True),
        (("key1", "key2"), True),
        (("key3", "key2"), True),
        (("key1", "key2", "key3"), True),
        (("key3",), True),
        (("key4",), False),
        (("key1", "key4"), False),
    ],
)
def test_has_attrs_condition(attrs, result):
    event = UniversalMessage(
        "test.TestEvent", "EVENT", payload=dict(key1=123, key2="xyz", key3=bool)
    )

    condition = HasAttrs(*attrs)
    assert condition.check(event) is result


def test_and_condition():
    event = UniversalMessage(
        "test.TestEvent", "EVENT", payload=dict(key1=123, key2="xyz", key3=True)
    )

    condition = And(HasAttrs("key1"), HasAttrs("key2", "key3"))
    assert condition.check(event) is True

    condition = And(condition, HasAttrs("key4"))
    assert condition.check(event) is False


@pytest.mark.parametrize("value", (1, "a", True, None))
def test_fail_use_not_condition_class_in_and_condition(value):
    with pytest.raises(TypeError):
        And(value)


def test_or_condition():
    event = UniversalMessage(
        "test.TestEvent", "EVENT", payload=dict(key1=123, key2="xyz", key3=True)
    )

    condition = Or(HasAttrs("key1"), HasAttrs("key2", "key3"))
    assert condition.check(event) is True

    condition = Or(HasAttrs("key4"), condition)
    assert condition.check(event) is True

    condition = And(HasAttrs("key4"), HasAttrs("key5"))
    assert condition.check(event) is False


@pytest.mark.parametrize("value", (1, "a", True, None))
def test_fail_use_not_condition_class_in_or_condition(value):
    with pytest.raises(TypeError):
        Or(value)


def test_not_condition():
    event = UniversalMessage(
        "test.TestEvent", "EVENT", payload=dict(key1=123, key2="xyz", key3=True)
    )

    condition = Not(HasAttrs("key1"))
    assert condition.check(event) is False

    condition = Not(HasAttrs("key4"))
    assert condition.check(event) is True


@pytest.mark.parametrize("value", (1, "a", True, None))
def test_fail_use_not_condition_class_in_not_condition(value):
    with pytest.raises(TypeError):
        Not(value)


def test_equal_condition():
    event = UniversalMessage(
        "test.TestEvent", "EVENT", payload=dict(key1=123, key2="xyz", key3=True)
    )

    condition = Equal(key1=123, key2="xyz", key3=True)
    assert condition.check(event) is True

    condition = Equal(key1=123, key2="xyz", key3=True, key4=None)
    assert condition.check(event) is False

    condition = Equal(key1=456, key2="xyz", key3=True)
    assert condition.check(event) is False

    condition = Equal(key1=123, key2="abc", key3=True)
    assert condition.check(event) is False

    condition = Equal(key1=123, key2="xyz", key3=False)
    assert condition.check(event) is False


def test_null_condition():
    event = UniversalMessage(
        "test.TestEvent", "EVENT", payload=dict(key1=123, key2="xyz", key3=True)
    )
    assert none_condition.check(event) is True
    event = UniversalMessage("test.TestEvent", "EVENT", payload={})
    assert none_condition.check(event) is True
