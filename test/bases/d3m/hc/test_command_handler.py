import datetime as dt
import typing as t
from typing import Callable
from uuid import UUID, uuid4

import pytest
from d3m.core import (
    IMessageMeta,
    DomainName,
    MessageName,
    MessageType,
)
from d3m.hc.command_handler import (
    ICommandHandler,
    CommandHandler,
)


class TestCommandHandler:
    def test_instance(self):
        handler = CommandHandler()
        assert isinstance(handler, Callable)
        assert isinstance(handler, ICommandHandler)
        with pytest.raises(AttributeError):
            _ = handler.command_class
        assert handler.origin_func is None

    def test_set_function(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class):
            pass

        handler = CommandHandler()
        assert handler.set_function(test_handler) is handler
        assert handler.origin_func is test_handler
        assert handler.command_class is command_class

    def test_fail_set_function_without_command_annotation(self):
        async def test_handler(cmd: int):
            pass

        with pytest.raises(
            AttributeError,
            match="Handler function required has AbstractCommand annotated parameter",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_fail_set_function_with_event_annotation(self):
        class FakeEventMeta(IMessageMeta):
            @property
            def __domain_name__(cls) -> DomainName:
                return DomainName("test")

            @property
            def __message_name__(cls) -> MessageName:
                return MessageName(cls.__name__)

            @property
            def __type__(cls) -> MessageType:
                return MessageType.EVENT

            def load(
                cls,
                payload: t.Union[t.Mapping, str, bytes],
                reference: UUID = None,
                timestamp: dt.datetime = None,
            ) -> "FakeEvent":
                pass

        class FakeEvent(metaclass=FakeEventMeta):
            pass

        async def test_handler(event: FakeEvent):
            pass

        with pytest.raises(
            AttributeError,
            match="Handler function required has AbstractCommand annotated parameter",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_set_function_with_multiple_command_annotation(
        self, command_class_builder
    ):
        command_class = command_class_builder()

        async def test_handler(cmd1: command_class, cmd2: command_class):
            pass

        with pytest.raises(
            AttributeError,
            match="Expected one AbstractCommand annotated parameter got 2",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_set_function_without_not_annotated_parameters(
        self, command_class_builder
    ):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class, arg1, arg2: str):
            pass

        with pytest.raises(
            AttributeError,
            match="All parameters for handler function required be annotated",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_set_function_with_positional_arguments(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class, /, arg1: int, arg2: str):
            pass

        with pytest.raises(
            AttributeError,
            match="Not allowed use function with positional only arguments as handler",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_set_function_with_kwargs_arguments(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class, **kwargs):
            pass

        with pytest.raises(
            AttributeError,
            match="Not allowed use function with var keyword arguments as handler",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_set_function_with_args_arguments(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class, *args):
            pass

        with pytest.raises(
            AttributeError,
            match="Not allowed use function with var positional arguments as handler",
        ):
            CommandHandler().set_function(test_handler)

    def test_fail_set_function_if_function_already_set(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class):
            pass

        handler = CommandHandler().set_function(test_handler)

        with pytest.raises(RuntimeError, match="function already set"):
            handler.set_function(test_handler)

    async def test_set_command(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class):
            return command

        handler = CommandHandler().set_function(test_handler)
        cmd = command_class.load({})
        handler2 = handler.with_command(cmd)

        assert handler2 is not handler
        assert handler2.origin_func is handler.origin_func is test_handler
        assert (await handler2()) is cmd

    def test_fail_set_command_with_invalid_type(self, command_class_builder):
        command_class1 = command_class_builder("TestCommand1")
        command_class2 = command_class_builder("TestCommand2")

        async def test_handler(cmd: command_class1):
            pass

        handler = CommandHandler().set_function(test_handler)

        with pytest.raises(
            TypeError,
            match=f"Invalid command type, expected {command_class1!r} got {command_class2!r}",
        ):
            handler.with_command(command_class2.load({}))

    def test_fail_set_command_before_set_function(self, command_class_builder):
        command_class1 = command_class_builder("TestCommand1")
        handler = CommandHandler()
        with pytest.raises(
            RuntimeError,
            match='Required call "set_function" before exec current method',
        ):
            handler.with_command(command_class1.load({}))

    def test_fail_set_command_if_command_is_already_set(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(cmd: command_class):
            pass

        handler = (
            CommandHandler()
            .set_function(test_handler)
            .with_command(command_class.load({}))
        )

        with pytest.raises(RuntimeError, match="Command context already set"):
            handler.with_command(command_class.load({}))

    async def test_handler_command_on_any_position(self, command_class_builder):
        command_class1 = command_class_builder("TestCommand1")
        cmd1 = command_class1.load({})
        command_class2 = command_class_builder("TestCommand2")
        cmd2 = command_class2.load({})
        command_class3 = command_class_builder("TestCommand3")
        cmd3 = command_class3.load({})

        async def test_handler1(cmd: command_class1, arg1: int, arg2: str):
            return cmd

        async def test_handler2(arg1: int, command: command_class2, arg2: str):
            return command

        async def test_handler3(arg1: int, arg2: str, any_name: command_class3):
            return any_name

        handler1 = (
            CommandHandler()
            .set_function(test_handler1)
            .with_command(cmd1)
            .with_defaults(arg1=123, arg2="abc")
        )
        assert (await handler1()) is cmd1

        handler2 = (
            CommandHandler()
            .set_function(test_handler2)
            .with_command(cmd2)
            .with_defaults(arg1=123, arg2="abc")
        )
        assert (await handler2()) is cmd2

        handler3 = (
            CommandHandler()
            .set_function(test_handler3)
            .with_command(cmd3)
            .with_defaults(arg1=123, arg2="abc")
        )
        assert (await handler3()) is cmd3

    async def test_fail_call_handler_without_command_context(
        self, command_class_builder
    ):
        command_class = command_class_builder()

        async def test_handler(command: command_class):
            return command

        handler = CommandHandler().set_function(test_handler)

        with pytest.raises(
            RuntimeError, match="Required set command before execute handler"
        ):
            await handler()

    async def test_fail_call_handler_without_set_arguments(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg1: str, arg2: int = 1):
            pass

        cmd = command_class.load({})
        handler = CommandHandler().set_function(test_handler).with_command(cmd)

        with pytest.raises(
            TypeError, match="missing 1 required positional argument: 'arg1'"
        ):
            await handler()

    async def test_set_defaults_values(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg: str):
            return command, arg

        cmd = command_class.load({})
        argument = str(uuid4())
        handler1 = CommandHandler().set_function(test_handler).with_command(cmd)
        handler2 = handler1.with_defaults(arg=argument)

        assert handler2 is not handler1
        assert handler2.origin_func == handler1.origin_func
        assert (await handler2()) == (cmd, argument)

    async def test_return_self_if_defaults_is_empty(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg: str):
            return command, arg

        cmd = command_class.load({})
        handler1 = CommandHandler().set_function(test_handler).with_command(cmd)
        handler2 = handler1.with_defaults()

        assert handler2 is handler1

    async def test_fail_set_defaults_with_invalid_types(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg: str):
            return command, arg

        with pytest.raises(
            TypeError,
            match=f'Invalid type of "arg" argument, expected {str!r} got {int!r}',
        ):
            CommandHandler().set_function(test_handler).with_defaults(arg=123)

    async def test_success_check_type_of_generics(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg: list[int]):
            return command, arg

        CommandHandler().set_function(test_handler).with_defaults(arg=[123])

    async def test_success_check_union_type(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg: int | str):
            return command, arg

        CommandHandler().set_function(test_handler).with_defaults(arg=123)
        CommandHandler().set_function(test_handler).with_defaults(arg="abc")

        async def test_handler(command: command_class, arg: t.Union[int, str]):
            return command, arg

        CommandHandler().set_function(test_handler).with_defaults(arg=123)
        CommandHandler().set_function(test_handler).with_defaults(arg="abc")

    async def test_success_check_optional_type(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg: int | None):
            return command, arg

        CommandHandler().set_function(test_handler).with_defaults(arg=123)
        CommandHandler().set_function(test_handler).with_defaults(arg=None)

        async def test_handler(command: command_class, arg: t.Optional[int]):
            return command, arg

        CommandHandler().set_function(test_handler).with_defaults(arg=123)
        CommandHandler().set_function(test_handler).with_defaults(arg=None)

    async def test_exclude_invalid_arguments(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(command: command_class, arg1: str, arg3: int):
            pass

        handler = (
            CommandHandler()
            .set_function(test_handler)
            .with_defaults(arg1="abc", arg2=False, arg3=123)
        )

        assert getattr(handler, "_defaults") == {
            "arg1": "abc",
            "arg3": 123,
        }

    async def test_set_defaults_from_signature(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(
            command: command_class, arg2: bool, arg1: str = "abc", arg3: int = 123
        ):
            pass

        handler = CommandHandler().set_function(test_handler)

        assert getattr(handler, "_defaults") == {
            "arg1": "abc",
            "arg3": 123,
        }

    async def test_dependencies_priority(self, command_class_builder):
        command_class = command_class_builder()

        async def test_handler(
            command: command_class,
            arg1: str = "abc",
            arg2: int = 123,
            arg3: t.Optional[bool] = None,
        ):
            return {"arg1": arg1, "arg2": arg2, "arg3": arg3}

        cmd = command_class.load({})
        handler1 = CommandHandler().set_function(test_handler).with_command(cmd)
        handler2 = handler1.with_defaults(arg1="xyz", arg3=True)
        handler3 = handler2.with_defaults(arg3=False)

        assert (await handler1()) == {"arg1": "abc", "arg2": 123, "arg3": None}

        assert (await handler2()) == {"arg1": "xyz", "arg2": 123, "arg3": True}

        assert (await handler3()) == {
            "arg1": "xyz",
            "arg2": 123,
            "arg3": False,
        }
