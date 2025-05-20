import datetime as dt
import uuid
from uuid import UUID, uuid4
import enum

import pytest
from decimal import Decimal

from pydantic_core import to_jsonable_python, to_json

from d3m.core import IMessage, DomainName, MessageName, UniversalMessage


class EnumTest(enum.Enum):
    COMMAND = "COMMAND"


class TestUniversalMessage:
    @pytest.mark.parametrize(
        "domain, name, type_, payload, reference, timestamp",
        [
            (
                "test.domain",
                "CustomCommand1",
                "command",
                {"key1": 123, "key2": "abc"},
                None,
                None,
            ),
            (
                DomainName("test.domain"),
                MessageName("CustomEvent"),
                "event",
                {"key4": 123, "key5": "abc"},
                uuid4(),
                dt.datetime.now(),
            ),
            (
                DomainName("test.domain"),
                MessageName("CustomCommand1"),
                "command",
                {"reference": uuid4()},
                uuid4(),
                dt.datetime.now(),
            ),
            (
                "test.domain.subdomain.sub-subdomain",
                "CustomCommand1",
                "command",
                {"key1": 123, "key2": "abc"},
                None,
                None,
            ),
        ],
    )
    def test_create_message(self, domain, name, type_, payload, reference, timestamp):
        msg = UniversalMessage(f"{domain}.{name}", type_, payload, reference, timestamp)
        assert isinstance(msg, IMessage)
        assert isinstance(msg.__domain_name__, DomainName)
        assert msg.__domain_name__ == str(domain)
        assert isinstance(msg.__message_name__, MessageName)
        assert msg.__message_name__ == str(name)
        assert msg.__type__ == type_.upper()
        assert msg.__payload__ == payload
        assert isinstance(msg.__reference__, UUID)
        assert reference is None or msg.__reference__ == reference
        assert isinstance(msg.__timestamp__, dt.datetime)
        assert msg.__timestamp__.tzinfo is not None
        assert timestamp is None or msg.__timestamp__.date() == timestamp.date()
        assert timestamp is None or msg.__timestamp__.time() == timestamp.time()
        assert timestamp is None or msg.__timestamp__.tzinfo == (
            timestamp.tzinfo or dt.timezone.utc
        )

    @pytest.mark.parametrize(
        "payload",
        [
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
                "dict": {"key1": uuid.UUID(int=2), "key2": 789},
                "enum": EnumTest.COMMAND,
                "other_object": UniversalMessage,
            }
        ],
    )
    def test_dump(self, payload):
        msg = UniversalMessage("test.CMD", "COMMAND", payload)
        assert msg.to_dict() == to_jsonable_python(payload, serialize_unknown=True)
        assert msg.to_json() == to_json(payload, serialize_unknown=True).decode()
        assert isinstance(msg.to_json(), str)
