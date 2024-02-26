import datetime as dt
import uuid
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from d3m.domain import DomainEvent, get_event_class
from d3m.core import (
    IMessage,
    IMessageMeta,
    DomainName,
    MessageName,
    MessageType,
)
from pydantic import BaseModel, ConfigDict, ValidationError
from typing_extensions import TypedDict


class TestDomainEvent:
    def test_fail_get_domain_of_base_class(self):
        with pytest.raises(AttributeError):
            _ = DomainEvent.__domain_name__

    def test_event_class_properties(self):
        class TestEvent(DomainEvent, domain="test-domain"):
            pass

        assert isinstance(TestEvent, IMessageMeta)
        assert issubclass(TestEvent, BaseModel)
        assert issubclass(TestEvent, IMessage)
        assert TestEvent.__domain_name__ == "test-domain"
        assert isinstance(TestEvent.__domain_name__, DomainName)
        assert TestEvent.__message_name__ == "TestEvent"
        assert isinstance(TestEvent.__message_name__, MessageName)
        assert TestEvent.__type__ == MessageType.EVENT

    def test_fail_create_event_with_out_domain(self):
        with pytest.raises(ValueError, match="required set domain name for event"):

            class Event(DomainEvent):
                pass

    def test_inherit_domain_from_parent_class(self):
        class TestEvent(DomainEvent, domain="test-domain.domain"):
            pass

        class TestSubEvent(TestEvent):
            pass

        assert TestSubEvent.__domain_name__ == "test-domain.domain"

    def test_fail_replace_domain_name_in_inheritance_class(self):
        class TestEvent(DomainEvent, domain="test-domain.domain"):
            pass

        with pytest.raises(RuntimeError) as exc:

            class TestSubEvent(TestEvent, domain="test-domain.other"):
                pass

        assert (
            str(exc.value)
            == "not allowed replace domain name in child class: domain.test_events.TestSubEvent"
        )

    def test_fail_multiple_inheritance_event(self):
        class TestDomainEvent1(DomainEvent, domain="test-domain.domain"):
            pass

        class TestDomainEvent2(DomainEvent, domain="test-domain.other"):
            pass

        with pytest.raises(
            RuntimeError, match="Not allowed multiple inheritance domain"
        ):

            class TestSubEvent(TestDomainEvent1, TestDomainEvent2):
                pass

    def test_event_instance_properties(self):
        class TestEvent(DomainEvent, domain="test-domain"):
            pass

        event = TestEvent()

        assert isinstance(event, BaseModel)
        assert isinstance(event, IMessage)
        assert event.__domain_name__ == "test-domain"
        assert isinstance(event.__domain_name__, DomainName)
        assert event.__message_name__ == "TestEvent"
        assert isinstance(event.__message_name__, MessageName)
        assert event.__type__ == MessageType.EVENT
        assert isinstance(event.__reference__, UUID)
        assert isinstance(event.__timestamp__, dt.datetime)
        assert isinstance(event.__payload__, dict)
        assert event.__payload__ == {}

    def test_event_not_change_properties(self):
        class TestEvent(DomainEvent, domain="test-domain"):
            pass

        event = TestEvent()

        assert event.__reference__ == event.__reference__
        assert event.__timestamp__ == event.__timestamp__

    def test_event_fields(self):
        class TestEvent(DomainEvent, domain="test-domain"):
            arg1: int
            arg2: str

        event = TestEvent(arg1=123, arg2="test")

        assert event.arg1 == 123
        assert event.arg2 == "test"

    @pytest.mark.parametrize("arg1, arg2", ((123, "abc"), (456, "xyz"), (789, "test")))
    def test_load_event(self, arg1, arg2):
        class TestEvent(DomainEvent, domain="test-domain"):
            arg1: int
            arg2: str

        reference = uuid4()
        timestamp = dt.datetime.now(dt.timezone.utc)

        event = TestEvent.load(
            {"arg1": arg1, "arg2": arg2}, reference=reference, timestamp=timestamp
        )

        assert event.arg1 == arg1
        assert event.arg2 == arg2
        assert event.__reference__ == reference
        assert event.__timestamp__ == timestamp
        assert event.__payload__ == {"arg1": arg1, "arg2": arg2}

    def test_load_event_without_reference_and_timestamp(self):
        class TestEvent(DomainEvent, domain="test-domain"):
            pass

        event = TestEvent.load({})

        assert isinstance(event.__reference__, UUID)
        assert isinstance(event.__timestamp__, dt.datetime)

    @pytest.mark.parametrize("arg1, arg2", ((123, "abc"), (456, "xyz"), (789, "test")))
    def test_dict_dump_event(self, arg1, arg2):
        class TestEvent(DomainEvent, domain="test-domain"):
            arg1: int
            arg2: str

        event = TestEvent(arg1=arg1, arg2=arg2)
        assert event.to_dict() == {
            "arg1": arg1,
            "arg2": arg2,
        }

    @pytest.mark.parametrize("arg1, arg2", ((123, "abc"), (456, "xyz"), (789, "test")))
    def test_json_dump_event(self, arg1, arg2):
        class TestEvent(DomainEvent, domain="test-domain"):
            arg1: int
            arg2: str

        event = TestEvent(arg1=arg1, arg2=arg2)
        assert event.to_json() == '{"arg1":' + str(arg1) + ',"arg2":"' + arg2 + '"}'

    def test_event_is_frozen(self):
        class TestEvent(DomainEvent, domain="test-domain"):
            arg1: int
            arg2: str

            model_config = ConfigDict(frozen=False)

        event = TestEvent(arg1=123, arg2="abc")

        with pytest.raises(ValidationError):
            event.arg1 = 456
        with pytest.raises(ValidationError):
            event.arg2 = "789"

    @pytest.mark.parametrize(
        "payload, result_dict, json_string",
        [
            (
                {
                    "date": dt.date(2023, 1, 1),
                    "uuid": uuid.UUID(int=1),
                    "datetime": dt.datetime(2023, 1, 1, 1, 2, 3),
                    "datetime_with_tz": dt.datetime(
                        2023, 1, 1, 1, 2, 3, 789, dt.timezone.utc
                    ),
                    "bool": True,
                    "int": 123,
                    "float": 123.56,
                    "decimal": Decimal("100.01"),
                    "string": "abc",
                    "list": [123, 456],
                    "set": {456, 123},
                    "tuple": (123, 456),
                    "dict_": {"key1": uuid.UUID(int=2), "key2": 789},
                    "enum": MessageType.EVENT,
                    # 'other_object': DomainEvent
                },
                {
                    "date": "2023-01-01",
                    "uuid": "00000000-0000-0000-0000-000000000001",
                    "datetime": "2023-01-01T01:02:03",
                    "datetime_with_tz": "2023-01-01T01:02:03.000789Z",
                    "bool": True,
                    "int": 123,
                    "float": 123.56,
                    "decimal": "100.01",
                    "string": "abc",
                    "list": [123, 456],
                    "set": [456, 123],
                    "tuple": [123, 456],
                    "dict_": {
                        "key1": "00000000-0000-0000-0000-000000000002",
                        "key2": 789,
                    },
                    "enum": "EVENT",
                },
                (
                    '{"date":"2023-01-01",'
                    '"uuid":"00000000-0000-0000-0000-000000000001",'
                    '"datetime":"2023-01-01T01:02:03",'
                    '"datetime_with_tz":"2023-01-01T01:02:03.000789Z",'
                    '"bool":true,"int":123,'
                    '"float":123.56,"decimal":"100.01",'
                    '"string":"abc",'
                    '"list":[123,456],'
                    '"set":[456,123],'
                    '"tuple":[123,456],'
                    '"dict_":{"key1":"00000000-0000-0000-0000-000000000002","key2":789},'
                    '"enum":"EVENT"}'
                ),
            ),
        ],
    )
    def test_dump(self, payload, result_dict, json_string):
        class Dict(TypedDict):
            key1: UUID
            key2: int

        class TestEvent(DomainEvent, domain="test"):
            date: dt.date
            uuid: uuid.UUID
            datetime: dt.datetime
            datetime_with_tz: dt.datetime
            bool: bool
            int: int
            float: float
            decimal: Decimal
            string: str
            list: list
            set: set
            tuple: tuple
            dict_: Dict
            enum: MessageType
            # other_object: object

        msg = TestEvent.load(payload)

        assert msg.to_dict() == result_dict
        assert msg.to_json() == json_string
        assert isinstance(msg.to_json(), str)

    def test_fail_create_identical_events(self):
        class TestEvent(DomainEvent, domain="test"):
            pass

        with pytest.raises(
            RuntimeError,
            match='Other event class for "test" domain with name "TestEvent" already registered',
        ):

            class TestEvent(DomainEvent, domain="test"):  # noqa
                pass

    def test_register_event(self):
        class EventTest(DomainEvent, domain="test-event"):
            pass

        assert get_event_class("test-event", "EventTest") is EventTest

    def test_fail_get_unregistered_event(self):
        with pytest.raises(
            RuntimeError,
            match=(
                'event class for "test-fail-get-unregistered-event" '
                'domain with name "EventTest" not registered'
            ),
        ):
            get_event_class("test-fail-get-unregistered-event", "EventTest")

    def test_fail_double_register_event(self):
        class EventTest(DomainEvent, domain="test-event"):
            pass

        with pytest.raises(
            RuntimeError,
            match=(
                'Other event class for "test-event" '
                'domain with name "EventTest" already registered'
            ),
        ):

            class EventTest(DomainEvent, domain="test-event"):  # noqa
                pass
