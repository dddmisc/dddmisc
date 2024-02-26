import asyncio
import typing as t
from copy import copy
from uuid import UUID, uuid4

import pytest
import tenacity as tc

from d3m.core import IHandlersCollection, UniversalMessage
from d3m.hc import Equal, HandlersCollection


class TestHandlersCollection:
    def test_type_of_instance(self):
        collection = HandlersCollection()

        assert isinstance(collection, IHandlersCollection)

    async def test_register_without_retry(self, command_class_builder):
        collection = HandlersCollection()
        cmd_class1 = command_class_builder("TestCommand1", "test")
        cmd_class2 = command_class_builder("TestCommand2", "test")
        cmd_class3 = command_class_builder("TestCommand3", "test")

        @collection.register
        async def test_handler1(cmd: cmd_class1, arg1: int, agr2: str):
            return cmd

        @collection.register
        async def test_handler2(arg1: int, cmd: cmd_class2, agr2: str):
            return cmd

        @collection.register
        async def test_handler3(arg1: int, agr2: str, cmd: cmd_class3):
            return cmd

        cmd1 = cmd_class1.load({})
        h = collection.get_command_handler(cmd1, arg1=123, agr2="abc")
        assert (await h()) == cmd1

        cmd2 = cmd_class2.load({})
        h = collection.get_command_handler(cmd2, arg1=123, agr2="abc")
        assert (await h()) == cmd2

        cmd3 = cmd_class3.load({})
        h = collection.get_command_handler(cmd3, arg1=123, agr2="abc")
        assert (await h()) == cmd3

    async def test_register_with_retry(self, command_class_builder):
        collection = HandlersCollection()
        cmd_class = command_class_builder()

        class TestException(BaseException):
            pass

        results = []

        @collection.register
        @tc.retry(
            stop=tc.stop_after_attempt(3),
            wait=tc.wait_fixed(0.1),
            retry=tc.retry_if_exception_type(TestException),
            reraise=True,
        )
        async def test_handler(command: cmd_class):
            results.append(command)
            raise TestException()

        cmd = cmd_class.load({})
        handler = collection.get_command_handler(cmd)

        with pytest.raises(TestException):
            await handler()

        assert results == [cmd, cmd, cmd]

    async def test_convert_command(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test")
        collection = HandlersCollection()

        @collection.register
        async def use_case(cmd: cmd_class):
            return cmd

        command = UniversalMessage("test.TestCommand", "COMMAND", {})

        handler = collection.get_command_handler(command)
        result = await handler()
        assert isinstance(result, cmd_class)
        assert result is not command
        assert result.__reference__ == command.__reference__
        assert result.__timestamp__ == command.__timestamp__

        command = cmd_class.load({})
        handler = collection.get_command_handler(command)
        result = await handler()
        assert result is command

    def test_fail_multiple_register_handler_for_one_command(
        self, command_class_builder
    ):
        cmd_class = command_class_builder("TestCommand", "test")
        collection = HandlersCollection()

        @collection.register
        async def use_case(cmd: cmd_class):
            return cmd

        with pytest.raises(
            RuntimeError, match=f"Handler for command {cmd_class!r} already registered"
        ):

            @collection.register
            async def use_case2(cmd: cmd_class):
                return cmd

    async def test_subscribe_to_event(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test", {"arg1": UUID})
        collection = HandlersCollection()

        @collection.subscribe("test.TestEvent")
        @collection.register
        async def use_case(cmd: cmd_class):
            return cmd

        event = UniversalMessage("test.TestEvent", "EVENT", {"arg1": uuid4()})
        handler = collection.get_event_handlers(event)[0]
        result = await handler()
        assert isinstance(result, cmd_class)
        assert result.arg1 == str(event.__payload__["arg1"])

    async def test_subscribe_conditions(self, command_class_builder):
        cmd_class1 = command_class_builder("TestCommand1", "test")
        cmd_class2 = command_class_builder("TestCommand2", "test")
        collection = HandlersCollection()

        @collection.subscribe("test.TestEvent", condition=Equal(key1=123))
        @collection.register
        async def use_case1(cmd: cmd_class1):
            return cmd

        @collection.subscribe("test.TestEvent", condition=Equal(key2=456))
        @collection.register
        async def use_case2(cmd: cmd_class2):
            return cmd

        event = UniversalMessage(
            "test.TestEvent",
            "EVENT",
            {
                "key1": 123,
                "key2": 789,
            },
        )
        handlers = collection.get_event_handlers(event)
        assert len(handlers) == 1
        assert isinstance(await handlers[0](), cmd_class1)

        event = UniversalMessage(
            "test.TestEvent",
            "EVENT",
            {
                "key1": 789,
                "key2": 456,
            },
        )

        handlers = collection.get_event_handlers(event)
        assert len(handlers) == 1
        assert isinstance(await handlers[0](), cmd_class2)

        event = UniversalMessage(
            "test.TestEvent",
            "EVENT",
            {
                "key1": 123,
                "key2": 456,
            },
        )

        handlers = collection.get_event_handlers(event)
        assert len(handlers) == 2
        events = await asyncio.gather(*(h() for h in handlers))
        assert set(type(event) for event in events) == {cmd_class1, cmd_class2}

        event = UniversalMessage(
            "test.TestEvent",
            "EVENT",
            {
                "key1": 789,
                "key2": 789,
            },
        )

        handlers = collection.get_event_handlers(event)
        assert len(handlers) == 0

    async def test_subscribe_converter(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"key": uuid4()})
        collection = HandlersCollection()

        @collection.subscribe("test.TestEvent", converter=lambda x: {"value": x["key"]})
        @collection.register
        async def test_handler(cmd: cmd_class):
            return cmd

        handlers = collection.get_event_handlers(event)
        result = await handlers[0]()
        assert isinstance(result, cmd_class)
        assert result.__payload__ == {"value": str(event.__payload__["key"])}

    async def test_logging_fail_convert_event(
        self, command_class_builder, list_logger_handler
    ):
        logs = list_logger_handler("d3m.hc")
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"key": uuid4()})
        collection = HandlersCollection()

        @collection.subscribe("test.TestEvent")
        @collection.register
        async def test_handler(cmd: cmd_class):
            return cmd

        handlers = collection.get_event_handlers(event)
        assert handlers == ()
        assert len(logs) == 1

        log_record = logs[0]  # type: logging.LogRecord
        assert log_record.message == 'Fail build handler for event "test.TestEvent"'
        assert log_record.exc_info[0] == TypeError
        assert log_record.levelname == "ERROR"

    async def test_subscribe_retry(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"value": uuid4()})
        collection = HandlersCollection()

        class TestException(Exception):
            pass

        results = []

        @collection.subscribe(
            "test.TestEvent",
            retry=tc.retry_if_exception_type(TestException),
            stop=tc.stop_after_attempt(3),
        )
        @collection.register
        async def test_handler(cmd: cmd_class):
            results.append(cmd)
            raise TestException()

        with pytest.raises(TestException):
            await collection.get_event_handlers(event)[0]()

        assert len(results) == 3
        assert isinstance(results[0], cmd_class)
        assert results[0] is results[1] is results[2]

    def test_fail_subscribe_without_register_command_handlers(
        self, command_class_builder
    ):
        cmd_class = command_class_builder()
        collection1 = HandlersCollection()
        collection2 = HandlersCollection()

        with pytest.raises(RuntimeError) as err:

            @collection2.subscribe("test.Event")
            @collection1.register
            async def handler(cmd: cmd_class):
                pass

        assert str(err.value) == (
            f"Handler for command ({cmd_class.__domain_name__}.{cmd_class.__message_name__}) "
            f"not registered in collection ({collection2})"
        )

    def test_fail_get_command_handler_for_not_registered_command(
        self, command_class_builder
    ):
        cmd_class = command_class_builder("TestCommand")
        collection = HandlersCollection()

        @collection.register
        async def handler(cmd: cmd_class):
            pass

        cmd = UniversalMessage("test.TestCommand2", "command", {})
        with pytest.raises(
            RuntimeError,
            match='Handler for command "test.TestCommand2" not registered in collection',
        ):
            _ = collection.get_command_handler(cmd)

    def test_repr(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand")
        collection = HandlersCollection()

        assert repr(collection) == (
            "d3m.hc.HandlersCollection"
            "<name=None, registered_command=0, registered_events=0>"
        )

        @collection.register
        async def handler(cmd: cmd_class):
            pass

        assert repr(collection) == (
            "d3m.hc.HandlersCollection"
            "<name=None, registered_command=1, registered_events=0>"
        )

        collection.subscribe("test.TestEvent")(handler)

        assert repr(collection) == (
            "d3m.hc.HandlersCollection"
            "<name=None, registered_command=1, registered_events=1>"
        )

        collection.subscribe("test.TestEvent2")(handler)

        assert repr(collection) == (
            "d3m.hc.HandlersCollection"
            "<name=None, registered_command=1, registered_events=2>"
        )

        assert repr(collection) == (
            "d3m.hc.HandlersCollection"
            "<name=None, registered_command=1, registered_events=2>"
        )

        assert repr(HandlersCollection(name="test collection")) == (
            "d3m.hc.HandlersCollection"
            "<name='test collection', registered_command=0, registered_events=0>"
        )

    async def test_set_defaults_to_command_handlers(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test-domain")
        collection = HandlersCollection()

        @collection.register
        async def use_case(cmd: cmd_class, key1: int, key2: str):
            return {"key1": key1, "key2": key2}

        collection.set_defaults("test-domain", key1=123, key2="abc")

        command = cmd_class.load({})

        handler = collection.get_command_handler(command)
        assert await handler() == {"key1": 123, "key2": "abc"}

        handler = collection.get_command_handler(command, key1=456)
        assert await handler() == {"key1": 456, "key2": "abc"}

        handler = collection.get_command_handler(command, key2="xyz")
        assert await handler() == {"key1": 123, "key2": "xyz"}

    async def test_set_defaults_to_event_handlers(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test-domain")
        collection = HandlersCollection()
        event = UniversalMessage("test.Event", "EVENT", {})

        @collection.subscribe("test.Event")
        @collection.register
        async def use_case(cmd: cmd_class, key1: int, key2: str):
            return {"key1": key1, "key2": key2}

        collection.set_defaults("test-domain", key1=123, key2="abc")

        handler = collection.get_event_handlers(event)[0]
        assert await handler() == {"key1": 123, "key2": "abc"}

        handler = collection.get_event_handlers(event, key1=456)[0]
        assert await handler() == {"key1": 456, "key2": "abc"}

        handler = collection.get_event_handlers(event, key2="xyz")[0]
        assert await handler() == {"key1": 123, "key2": "xyz"}

    async def test_different_defaults_for_different_domains(
        self, command_class_builder
    ):
        cmd_class1 = command_class_builder("TestCommand", "test-domain-one")
        cmd_class2 = command_class_builder("TestCommand", "test-domain-two")
        collection = HandlersCollection()

        @collection.register
        async def use_case1(_: cmd_class1, key1: int, key2: str):
            return {"key1": key1, "key2": key2}

        @collection.register
        async def use_case2(_: cmd_class2, key1: int, key2: str):
            return {"key1": key1, "key2": key2}

        collection.set_defaults("test-domain-one", key1=123, key2="abc")
        collection.set_defaults("test-domain-two", key1=789, key2="xyz")

        command = cmd_class1.load({})
        handler = collection.get_command_handler(command)
        assert await handler() == {"key1": 123, "key2": "abc"}

        command = cmd_class2.load({})
        handler = collection.get_command_handler(command)
        assert await handler() == {"key1": 789, "key2": "xyz"}

    async def test_update_defaults(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test-domain")
        collection = HandlersCollection()

        @collection.register
        async def use_case(_: cmd_class, key1: int, key2: str, key3: bool):
            return {"key1": key1, "key2": key2, "key3": key3}

        collection.set_defaults("test-domain", key1=123, key3=True)
        collection.set_defaults("test-domain", key1=789, key2="xyz")

        command = cmd_class.load({})
        handler = collection.get_command_handler(command)
        assert await handler() == {
            "key1": 789,
            "key2": "xyz",
            "key3": True,
        }


class TestDefaults:
    async def test_handler_defaults_priority(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test-domain")
        collection = HandlersCollection()

        @collection.register
        async def use_case(
            _: cmd_class,
            key1: int = 123,
            key2: str = "abc",
            key3: t.Optional[bool] = None,
        ):
            return {"key1": key1, "key2": key2, "key3": key3}

        command = cmd_class.load({})
        handler = collection.get_command_handler(command)
        assert await handler() == {
            "key1": 123,
            "key2": "abc",
            "key3": None,
        }

        handler = collection.get_command_handler(command, key3=False)
        assert await handler() == {
            "key1": 123,
            "key2": "abc",
            "key3": False,
        }

    async def test_set_default_priority(self, command_class_builder):
        cmd_class = command_class_builder("TestCommand", "test-domain")
        collection = HandlersCollection()

        @collection.register
        async def use_case(
            _: cmd_class,
            key1: int = 123,
            key2: str = "abc",
            key3: t.Optional[bool] = None,
        ):
            return {"key1": key1, "key2": key2, "key3": key3}

        collection.set_defaults("test-domain", key1=456, key2="xyz")
        collection.set_defaults("test-domain", key1=789, key2="xyz")

        command = cmd_class.load({})
        handler = collection.get_command_handler(command)
        assert await handler() == {
            "key1": 789,
            "key2": "xyz",
            "key3": None,
        }

        handler = collection.get_command_handler(command, key3=False)
        assert await handler() == {
            "key1": 789,
            "key2": "xyz",
            "key3": False,
        }

    async def test_copy_collection(self, command_class_builder):
        cmd_class1 = command_class_builder("TestCommand", "test-domain")
        cmd_class2 = command_class_builder("TestCommand2", "test-domain")
        collection = HandlersCollection()
        collection.set_defaults("test-domain", key1=456)

        @collection.register
        async def use_case1(
            _: cmd_class1,
            key1: int = 123,
            key2: str = "abc",
            key3: t.Optional[bool] = None,
        ):
            return {"key1": key1, "key2": key2, "key3": key3}

        copy_collection = copy(collection)
        collection.set_defaults("test-domain", key1=789)

        @collection.register
        async def use_case2(
            _: cmd_class2,
        ):
            pass

        result = await collection.get_command_handler(cmd_class1())()
        assert result == {"key1": 789, "key2": "abc", "key3": None}

        result = await copy_collection.get_command_handler(cmd_class1())()
        assert result == {"key1": 456, "key2": "abc", "key3": None}

        with pytest.raises(RuntimeError):
            copy_collection.get_command_handler(cmd_class2())
