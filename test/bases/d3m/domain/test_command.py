import datetime as dt
import uuid
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from d3m.domain import get_command_class, DomainCommand
from d3m.core import (
    IMessage,
    IMessageMeta,
    DomainName,
    MessageName,
    MessageType,
)
from pydantic import BaseModel, ConfigDict, ValidationError
from typing_extensions import TypedDict


class TestDomainCommand:
    def test_domain_of_base_class_is_none(self):
        with pytest.raises(AttributeError):
            _ = DomainCommand.__domain_name__

    def test_command_class_properties(self):
        class TestCommand(DomainCommand, domain="test-domain"):
            pass

        assert isinstance(TestCommand, IMessageMeta)
        assert issubclass(TestCommand, BaseModel)
        assert issubclass(TestCommand, IMessage)
        assert TestCommand.__domain_name__ == "test-domain"
        assert isinstance(TestCommand.__domain_name__, DomainName)
        assert TestCommand.__message_name__ == "TestCommand"
        assert isinstance(TestCommand.__message_name__, MessageName)
        assert TestCommand.__type__ == MessageType.COMMAND

    def test_fail_create_command_with_out_domain(self):
        with pytest.raises(ValueError, match="required set domain name for command"):

            class Command(DomainCommand):
                pass

    def test_inherit_domain_from_parent_class(self):
        class TestCommand(DomainCommand, domain="test-domain.domain"):
            pass

        class TestSubCommand(TestCommand):
            pass

        assert TestSubCommand.__domain_name__ == "test-domain.domain"

    def test_fail_replace_domain_name_in_inheritance_class(self):
        class TestCommand(DomainCommand, domain="test-domain.domain"):
            pass

        with pytest.raises(
            RuntimeError,
            match=r"not allowed replace domain name in child class\: domain\.test_command\.TestSubCommand",
        ):

            class TestSubCommand(TestCommand, domain="test-domain.other"):
                pass

    def test_fail_multiple_inheritance_command(self):
        class TestCommand1(DomainCommand, domain="test-domain.domain"):
            pass

        class TestCommand2(DomainCommand, domain="test-domain.other"):
            pass

        with pytest.raises(
            RuntimeError, match="Not allowed multiple inheritance domain"
        ):

            class TestSubCommand(TestCommand1, TestCommand2):
                pass

    def test_command_instance_properties(self):
        class TestCommand(DomainCommand, domain="test-domain"):
            pass

        cmd = TestCommand()

        assert isinstance(cmd, BaseModel)
        assert isinstance(cmd, IMessage)
        assert cmd.__domain_name__ == "test-domain"
        assert isinstance(cmd.__domain_name__, DomainName)
        assert cmd.__message_name__ == "TestCommand"
        assert isinstance(cmd.__message_name__, MessageName)
        assert cmd.__type__ == MessageType.COMMAND
        assert isinstance(cmd.__reference__, UUID)
        assert isinstance(cmd.__timestamp__, dt.datetime)
        assert isinstance(cmd.__payload__, dict)
        assert cmd.__payload__ == {}

    def test_command_not_change_properties(self):
        class TestCommand(DomainCommand, domain="test-domain"):
            pass

        cmd = TestCommand()

        assert cmd.__reference__ == cmd.__reference__
        assert cmd.__timestamp__ == cmd.__timestamp__

    def test_command_fields(self):
        class TestCommand(DomainCommand, domain="test-domain"):
            arg1: int
            arg2: str

        cmd = TestCommand(arg1=123, arg2="test")

        assert cmd.arg1 == 123
        assert cmd.arg2 == "test"

    @pytest.mark.parametrize("arg1, arg2", ((123, "abc"), (456, "xyz"), (789, "test")))
    def test_load_command(self, arg1, arg2):
        class TestCommand(DomainCommand, domain="test-domain"):
            arg1: int
            arg2: str

        reference = uuid4()
        timestamp = dt.datetime.now(dt.timezone.utc)

        cmd = TestCommand.load(
            {"arg1": arg1, "arg2": arg2}, reference=reference, timestamp=timestamp
        )

        assert cmd.arg1 == arg1
        assert cmd.arg2 == arg2
        assert cmd.__reference__ == reference
        assert cmd.__timestamp__ == timestamp
        assert cmd.__payload__ == {"arg1": arg1, "arg2": arg2}

    def test_load_command_without_reference_and_timestamp(self):
        class TestCommand(DomainCommand, domain="test-domain"):
            pass

        cmd = TestCommand.load({})

        assert isinstance(cmd.__reference__, UUID)
        assert isinstance(cmd.__timestamp__, dt.datetime)

    @pytest.mark.parametrize("arg1, arg2", ((123, "abc"), (456, "xyz"), (789, "test")))
    def test_dict_dump_command(self, arg1, arg2):
        class TestCommand(DomainCommand, domain="test-domain"):
            arg1: int
            arg2: str

        cmd = TestCommand(arg1=arg1, arg2=arg2)
        assert cmd.to_dict() == {
            "arg1": arg1,
            "arg2": arg2,
        }

    @pytest.mark.parametrize("arg1, arg2", ((123, "abc"), (456, "xyz"), (789, "test")))
    def test_json_dump_command(self, arg1, arg2):
        class TestCommand(DomainCommand, domain="test-domain"):
            arg1: int
            arg2: str

        cmd = TestCommand(arg1=arg1, arg2=arg2)
        assert cmd.to_json() == '{"arg1":' + str(arg1) + ',"arg2":"' + arg2 + '"}'

    def test_command_is_frozen(self):
        class TestCommand(DomainCommand, domain="test-domain"):
            arg1: int
            arg2: str

            model_config = ConfigDict(frozen=False)

        cmd = TestCommand(arg1=123, arg2="abc")

        with pytest.raises(ValidationError):
            cmd.arg1 = 456
        with pytest.raises(ValidationError):
            cmd.arg2 = "789"

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
                    "enum": MessageType.COMMAND,
                    # 'other_object': DomainCommand
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
                    "enum": "COMMAND",
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
                    '"enum":"COMMAND"}'
                ),
            ),
        ],
    )
    def test_dump(self, payload, result_dict, json_string):
        class Dict(TypedDict):
            key1: UUID
            key2: int

        class TestCommand(DomainCommand, domain="test"):
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

        msg = TestCommand.load(payload)

        assert msg.to_dict() == result_dict
        assert msg.to_json() == json_string
        assert isinstance(msg.to_json(), str)

    def test_fail_create_identical_commands(self):
        class TestCommand(DomainCommand, domain="test"):
            pass

        with pytest.raises(
            RuntimeError,
            match=r'Other command class for "test" domain with name "TestCommand" already registered',
        ):

            class TestCommand(DomainCommand, domain="test"):  # noqa
                pass

    def test_get_command_class_function(self):
        class CommandTest(DomainCommand, domain="test-command"):
            pass

        assert get_command_class("test-command", "CommandTest") is CommandTest

    def test_read_only_attributes_of_command(self):
        class TestCommand(DomainCommand, domain="test"):
            arg: str
            model_config = ConfigDict(frozen=False)

        obj = TestCommand(arg="abc")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.arg = "xyz"
