import typing as t
from uuid import UUID, uuid4

import pytest
import tenacity as tc
from d3m.core import DomainName, MessageName, UniversalMessage
from d3m.hc import HasAttrs
from d3m.hc.command_handler import CommandHandler
from d3m.hc.conditions import none_condition
from d3m.hc.subscribe_config import SubscribeConfig


class TestSubscribeConfig:
    def test_init(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": int})

        cfg = SubscribeConfig(
            command=cmd_class,
            event_domain="test",
            event_name="TestEvent",
        )

        assert cfg.event_key == (DomainName("test"), MessageName("TestEvent"))
        assert cfg.command_key == (
            cmd_class.__domain_name__,
            cmd_class.__message_name__,
        )
        assert getattr(cfg, "_command") is cmd_class
        assert getattr(cfg, "_condition", none_condition)
        assert isinstance(getattr(cfg, "_converter"), t.Callable)

        retry = getattr(cfg, "_retry")
        assert isinstance(retry, tc.AsyncRetrying)
        assert retry.retry == tc.retry_never
        assert isinstance(retry.stop, tc.stop_after_attempt)
        assert retry.stop.max_attempt_number == 1
        assert isinstance(retry.wait, tc.wait_none)
        assert retry.reraise is True

        def converter(payload: t.Mapping):
            return {"value": payload["key"]}

        cfg = SubscribeConfig(
            command=cmd_class,
            event_domain="test.test",
            event_name="TestEvent2",
            condition=HasAttrs("key"),
            converter=converter,
            retry=tc.retry_if_exception_type(),
            wait=tc.wait_fixed(1),
            stop=tc.stop_never,
        )

        assert isinstance(getattr(cfg, "_condition"), HasAttrs)
        assert getattr(cfg, "_converter") is converter
        retry = getattr(cfg, "_retry")
        assert isinstance(retry.retry, tc.retry_if_exception_type)
        assert retry.stop is tc.stop_never
        assert isinstance(retry.wait, tc.wait_fixed)
        assert retry.reraise is True

    async def test_build_handler(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"value": uuid4()})
        cfg = SubscribeConfig(
            command=cmd_class, event_domain="test", event_name="TestEvent"
        )

        async def test_handler(cmd: cmd_class, arg1: int, arg2: str):
            return cmd, dict(arg1=arg1, arg2=arg2)

        cmd_handler = CommandHandler().set_function(test_handler)

        ev_handler = cfg.build_handler(event, cmd_handler, arg1=123, arg2="abc")

        command, attrs = await ev_handler()

        assert attrs == dict(arg1=123, arg2="abc")
        assert isinstance(command, cmd_class)
        assert command.to_dict() == event.to_dict()
        assert command.__reference__ != event.__reference__
        assert command.__timestamp__ != event.__timestamp__

    def test_build_handler_if_condition_false(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"value": uuid4()})
        cfg = SubscribeConfig(
            command=cmd_class,
            event_domain="test",
            event_name="TestEvent",
            condition=HasAttrs("key"),
        )

        async def test_handler(cmd: cmd_class):
            pass

        cmd_handler = CommandHandler().set_function(test_handler)

        ev_handler = cfg.build_handler(event, cmd_handler)

        assert ev_handler is None

    def test_fail_build_handler_from_handler_to_other_command(
        self, command_class_builder
    ):
        cmd_class1 = command_class_builder("TestCommand1")
        cmd_class2 = command_class_builder("TestCommand2")
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        cfg = SubscribeConfig(
            command=cmd_class1, event_domain="test", event_name="TestEvent"
        )

        async def test_handler(cmd: cmd_class2):
            pass

        cmd_handler = CommandHandler().set_function(test_handler)

        with pytest.raises(
            ValueError, match="Handler not register other command class"
        ):
            cfg.build_handler(event, cmd_handler)

    async def test_converter(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"key": uuid4()})
        cfg = SubscribeConfig(
            command=cmd_class,
            event_domain="test",
            event_name="TestEvent",
            converter=lambda x: {"value": x["key"]},
        )

        async def test_handler(cmd: cmd_class):
            return cmd

        cmd_handler = CommandHandler().set_function(test_handler)

        ev_handler = cfg.build_handler(event, cmd_handler)

        command = await ev_handler()
        assert command.to_dict()["value"] == event.to_dict()["key"]

    async def test_retry(self, command_class_builder):
        cmd_class = command_class_builder(annotation={"value": UUID})
        event = UniversalMessage("test.TestEvent", "EVENT", {"value": uuid4()})

        class TestException(Exception):
            pass

        cfg = SubscribeConfig(
            command=cmd_class,
            event_domain="test",
            event_name="TestEvent",
            retry=tc.retry_if_exception_type(TestException),
            stop=tc.stop_after_attempt(3),
        )

        results = []

        async def test_handler(cmd: cmd_class):
            results.append(cmd)
            raise TestException()

        cmd_handler = CommandHandler().set_function(test_handler)

        ev_handler = cfg.build_handler(event, cmd_handler)
        with pytest.raises(TestException):
            await ev_handler()
        assert len(results) == 3
        assert results[0] is results[1] is results[2]

    async def test_fail_build_handler_from_command(self, command_class_builder):
        cmd_class = command_class_builder()
        cfg = SubscribeConfig(
            command=cmd_class, event_domain="test", event_name="TestCommand"
        )

        async def test_handler(cmd: cmd_class):
            return cmd

        cmd_handler = CommandHandler().set_function(test_handler)

        command = cmd_class.load({})
        with pytest.raises(ValueError, match=f"Can not build handler from {command!r}"):
            cfg.build_handler(command, cmd_handler)

    async def test_fail_build_handler_from_other_event(self, command_class_builder):
        cmd_class = command_class_builder()
        event = UniversalMessage("test.OtherEvent", "EVENT", {})
        cfg = SubscribeConfig(
            command=cmd_class, event_domain="test", event_name="TestEvent"
        )

        async def test_handler(cmd: cmd_class):
            return cmd

        cmd_handler = CommandHandler().set_function(test_handler)

        with pytest.raises(ValueError) as exc:
            cfg.build_handler(event, cmd_handler)

        assert str(exc.value) == f"Can not build handler from {event!r}"
