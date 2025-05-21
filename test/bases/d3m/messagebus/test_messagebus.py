import asyncio
import functools
import logging
import queue
import threading
from contextlib import asynccontextmanager
from contextvars import ContextVar  # noqa
from uuid import uuid4

import pytest
from d3m.core import (
    IMessagebus,
    IMessagebusPolicy,
    get_running_messagebus,
    helpers,
    MessagebusEvents,
)

from d3m.core import UniversalMessage
from d3m.messagebus import Messagebus, MessagebusPolicy
from d3m.messagebus.collection import MessagebusHandlersCollection


class TestMessagebus:
    async def test_fail_run_without_handlers(self):
        mb = Messagebus()
        with pytest.raises(
            RuntimeError, match="Messagebus cannot be run without handlers"
        ):
            await mb.run()

    async def test_set_is_running_true_after_run(self, handler_collection_factory):
        """Тест изменения флага запуска после вызова run"""
        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        assert mb.is_running() is False

        await mb.run()
        assert mb.is_running() is True

    async def test_set_is_running_false_after_stop(self, handler_collection_factory):
        """Тест изменения флага запуска после вызова stop"""
        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        await mb.run()

        assert mb.is_running() is True

        await mb.stop()
        assert mb.is_running() is False

    async def test_run_messagebus_after_stop(self, handler_collection_factory):
        """Тест повторного запуска messagebus после остановки методом stop"""
        mb = Messagebus()
        mb.include_collection(handler_collection_factory())

        await mb.run()
        assert mb.is_running() is True

        await mb.stop()
        assert mb.is_running() is False

        await mb.run()
        assert mb.is_running() is True

    async def test_idempotency_run(self, handler_collection_factory):
        """Тест идемпотентности запуска messagebus командой run"""
        mb = Messagebus()
        mb.include_collection(handler_collection_factory())

        await mb.run()
        await mb.run()
        assert mb.is_running() is True

    async def test_register_running_messagebus(self, handler_collection_factory):
        """Тест регистрации запущенного messagebus после вызова команды run"""
        mb = Messagebus()
        mb.include_collection(handler_collection_factory())

        await mb.run()
        assert mb is get_running_messagebus()

    async def test_fail_running_many_messagebus(self, handler_collection_factory):
        """Тест не возможности запуска нескольких экземпляров messagebus в одном потоке"""
        mb1 = Messagebus()
        mb2 = Messagebus()
        mb1.include_collection(handler_collection_factory())
        mb2.include_collection(handler_collection_factory())
        await mb1.run()
        with pytest.raises(
            RuntimeError,
            match="Cannot run the messagebus while another messagebus is running",
        ):
            await mb2.run()

    async def test_include_handler_collection(self, handler_collection_factory):
        """Тест подключения коллекции обработчиков к messagebus"""

        cmd = UniversalMessage("test.TestCommand", "COMMAND", {})
        ev = UniversalMessage("test.TestEvent", "EVENT", {})
        collection1 = handler_collection_factory()
        collection2 = handler_collection_factory()

        async def test_handler(message):
            return message

        collection1.add_handler(cmd, test_handler)
        collection2.add_handler(ev, test_handler)

        mb = Messagebus()
        mb.include_collection(collection1)
        mb.include_collection(collection2)

        mb_collection = getattr(mb, "_collection")
        assert isinstance(mb_collection, MessagebusHandlersCollection)
        assert len(mb_collection) == 2
        assert mb_collection != collection1
        assert mb_collection != collection2

    async def test_handle_command_success(self, handler_collection_factory):
        cmd = UniversalMessage("test.TestCommand", "COMMAND", {})
        collection = handler_collection_factory()

        async def test_handler(message):
            return message

        collection.add_handler(cmd, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = mb.handle_message(cmd)

        assert isinstance(result, asyncio.Future)
        assert (await result) is cmd

    async def test_handle_command_with_dependencies(self, handler_collection_factory):
        cmd = UniversalMessage("test.TestCommand", "COMMAND", {})
        collection = handler_collection_factory()

        async def test_handler(message, **kwargs):
            return message, kwargs

        collection.add_handler(cmd, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = mb.handle_message(cmd, agr1="abc", arg2=123)

        assert isinstance(result, asyncio.Future)
        assert (await result) == (cmd, dict(agr1="abc", arg2=123))

    async def test_fail_handle_message_to_not_running_messagebus(self):
        cmd = UniversalMessage("test.TestCommand", "COMMAND", {})
        mb = Messagebus()
        with pytest.raises(RuntimeError, match="Messagebus is not running"):
            _ = mb.handle_message(cmd)

    @pytest.mark.parametrize("message_type", (123, "abc", None, 456.23, False))
    async def test_fail_handle_invalid_message_type(
        self, handler_collection_factory, message_type
    ):
        class FakeMessage:
            @property
            def __type__(self):
                return message_type

        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        await mb.run()

        with pytest.raises(TypeError, match="Unknown type of message"):
            _ = mb.handle_message(FakeMessage())

    async def test_handle_command_exception(self, handler_collection_factory):
        cmd = UniversalMessage("test.TestCommand", "COMMAND", {})
        collection = handler_collection_factory()

        async def test_handler(message):
            raise RuntimeError(message.__reference__)

        collection.add_handler(cmd, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = mb.handle_message(cmd)

        assert isinstance(result, asyncio.Future)
        with pytest.raises(RuntimeError, match=str(cmd.__reference__)):
            await result

    async def test_handle_not_registered_command(self, handler_collection_factory):
        cmd = UniversalMessage("test.TestCommand", "COMMAND", {})
        collection = handler_collection_factory()
        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()
        with pytest.raises(
            RuntimeError, match=r'Handler for command "test.TestCommand" not registered'
        ):
            _ = mb.handle_message(cmd)

    async def test_correct_get_handler(self, handler_collection_factory):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        cmd2 = UniversalMessage("test.TestCommand2", "COMMAND", {})
        collection = handler_collection_factory()

        async def test_handler1(_message):
            return "test_handler1"

        async def test_handler2(_message):
            return "test_handler2"

        collection.add_handler(cmd1, test_handler1)
        collection.add_handler(cmd2, test_handler2)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = await mb.handle_message(cmd1)
        assert result == "test_handler1"

        result = await mb.handle_message(cmd2)
        assert result == "test_handler2"

    async def test_handle_not_registered_event(self, handler_collection_factory):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()
        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()
        await mb.handle_message(event)
        assert getattr(mb, "_tasks") == {}

    async def test_handle_event_with_single_handler(self, handler_collection_factory):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()

        async def test_handler(message):
            return message

        collection.add_handler(event, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = mb.handle_message(event)
        assert isinstance(result, asyncio.Future)
        assert await result == [event]

    async def test_handle_event_with_multiple_handlers(
        self, handler_collection_factory
    ):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()

        values = set()
        for i in range(10):

            async def test_handler(_message):
                value = uuid4()
                values.add(value)
                return value

            collection.add_handler(event, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = await mb.handle_message(event)
        assert len(result) == 10
        assert set(result) == set(values)

    async def test_different_context_for_multiple_handlers(
        self, handler_collection_factory
    ):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()
        ctx = ContextVar("test", default=-1)
        values = {}
        for i in range(1, 10):

            async def test_handler(_message, index):
                ctx.set(index)
                await asyncio.sleep(0.1)
                values[index] = ctx.get()
                return index, ctx.get()

            collection.add_handler(event, functools.partial(test_handler, index=i))

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = await mb.handle_message(event)
        assert len(result) == 9
        assert values == {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9}

    async def test_not_raise_error_if_event_handler_not_registered(
        self, handler_collection_factory
    ):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = mb.handle_message(event)
        assert isinstance(result, asyncio.Future)
        assert await result == []

    async def test_return_event_handler_error(self, handler_collection_factory):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()

        error = RuntimeError(str(uuid4()))

        async def test_handler1(_message):
            raise error

        async def test_handler2(message):
            return message

        collection.add_handler(event, test_handler1)
        collection.add_handler(event, test_handler2)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = await mb.handle_message(event)
        assert set(result) == {error, event}

    async def test_handle_event_with_dependencies(self, handler_collection_factory):
        event = UniversalMessage("test.TestEvent", "EVENT", {})
        collection = handler_collection_factory()

        async def test_handler(message, **kwargs):
            return message, kwargs

        collection.add_handler(event, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()

        result = mb.handle_message(event, agr1="abc", arg2=123)

        assert isinstance(result, asyncio.Future)
        assert (await result) == [(event, dict(agr1="abc", arg2=123))]

    @pytest.mark.parametrize(
        "domain, name, type_",
        [
            ("test", "TestCommand", "COMMAND"),
            ("test", "TestEvent", "EVENT"),
        ],
    )
    async def test_stop_wait_all_tasks(
        self, domain, name, type_, handler_collection_factory
    ):
        event = UniversalMessage(f"{domain}.{name}", type_, {})
        collection = handler_collection_factory()

        results = []
        messages_count = 1000

        async def test_handler(message, **_kwargs):
            await asyncio.sleep(0.1)
            results.append(message)

        collection.add_handler(event, test_handler)

        mb = Messagebus()
        mb.include_collection(collection)
        await mb.run()
        for i in range(messages_count):
            ev = UniversalMessage(f"{domain}.{name}", type_, {})
            _ = mb.handle_message(ev)

        await mb.stop()
        assert len(results) == messages_count
        for res in results:
            assert isinstance(res, UniversalMessage)
            assert res.__domain_name__ == domain
            assert res.__message_name__ == name
            assert res.__type__ == type_

    async def test_handle_dependency_to_downstream_handlers(
        self, handler_collection_factory
    ):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})
        cmd2 = UniversalMessage("test.TestCommand2", "COMMAND", {})
        event2 = UniversalMessage("test.TestEvent2", "EVENT", {})

        collection = handler_collection_factory()

        async def cmd1_handler(message, results, **kwargs):
            mb = get_running_messagebus()
            _ = mb.handle_message(event1)
            results[message] = kwargs

        collection.add_handler(cmd1, cmd1_handler)

        async def event1_handler(message, results, **kwargs):
            mb = get_running_messagebus()
            _ = mb.handle_message(cmd2)
            results[message] = kwargs

        collection.add_handler(event1, event1_handler)

        async def cmd2_handler(message, results, **kwargs):
            mb = get_running_messagebus()
            _ = mb.handle_message(event2)
            results[message] = kwargs

        collection.add_handler(cmd2, cmd2_handler)

        async def event2_handler(message, results, **kwargs):
            results[message] = kwargs

        collection.add_handler(event2, event2_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)
        await messagebus.run()
        results1 = {}
        _ = messagebus.handle_message(cmd1, results=results1, arg1="abc", arg2=123)
        results2 = {}
        _ = messagebus.handle_message(cmd1, results=results2, arg3=789, arg4="xyz")

        assert results1 == results2 == {}
        await messagebus.stop()

        assert results1 == {
            cmd1: dict(arg1="abc", arg2=123),
            cmd2: dict(arg1="abc", arg2=123),
            event1: dict(arg1="abc", arg2=123),
            event2: dict(arg1="abc", arg2=123),
        }

        assert results2 == {
            cmd1: dict(arg3=789, arg4="xyz"),
            cmd2: dict(arg3=789, arg4="xyz"),
            event1: dict(arg3=789, arg4="xyz"),
            event2: dict(arg3=789, arg4="xyz"),
        }

    async def test_not_handel_context_events_to_closed_messagebus(
        self, handler_collection_factory
    ):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})

        collection = handler_collection_factory()

        async def cmd1_handler(message, results: set):
            results.add(message)
            mb = get_running_messagebus()
            _ = mb.handle_message(event1)

        collection.add_handler(cmd1, cmd1_handler)

        async def event1_handler(message, results: set):
            results.add(message)

        collection.add_handler(event1, event1_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)
        await messagebus.run()
        result = set()
        _ = messagebus.handle_message(cmd1, results=result)

        assert result == set()

        await messagebus.close()
        assert result == {cmd1}

    async def test_run_until_complete(self, handler_collection_factory):
        """Тест запуска messagebus методом run_until_complete"""
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})

        collection = handler_collection_factory()

        async def cmd1_handler(message, results: set):
            results.add(message)
            mb = get_running_messagebus()
            _ = mb.handle_message(event1)

        collection.add_handler(cmd1, cmd1_handler)

        async def event1_handler(message, results: set):
            results.add(message)

        collection.add_handler(event1, event1_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)

        result = set()
        _ = await messagebus.run_until_complete(cmd1, results=result)

        await messagebus.close()
        assert result == {cmd1, event1}

    async def test_fail_run_until_complete_event(self, handler_collection_factory):
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})

        collection = handler_collection_factory()

        async def event1_handler(message, results: set):
            results.add(message)

        collection.add_handler(event1, event1_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)

        with pytest.raises(TypeError, match="Invalid message type"):
            _ = await messagebus.run_until_complete(event1)

    async def test_double_call_stop(self, handler_collection_factory):
        event_loop = asyncio.get_running_loop()
        cmd = UniversalMessage("test.TestCommand1", "COMMAND", {})

        collection = handler_collection_factory()

        async def cmd1_handler(_message):
            await asyncio.sleep(0.1)

        collection.add_handler(cmd, cmd1_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)
        await messagebus.run()

        _ = messagebus.handle_message(cmd)
        task = event_loop.create_task(messagebus.stop())
        await messagebus.stop()
        await asyncio.sleep(0)
        assert task.done()

        await task

    async def test_wait_stopping_before_run(self, handler_collection_factory):
        cmd = UniversalMessage("test.TestCommand1", "COMMAND", {})

        collection = handler_collection_factory()
        result = []

        async def cmd1_handler(_message):
            await asyncio.sleep(0.01)
            result.append("task_completed")

        collection.add_handler(cmd, cmd1_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)
        await messagebus.run()

        _ = messagebus.handle_message(cmd)
        stop_coro = asyncio.create_task(messagebus.stop())
        await asyncio.sleep(0)
        assert len(result) == 0
        await messagebus.run()
        assert len(result) == 1
        await messagebus.stop()
        await stop_coro

    async def test_close_not_running_messagebus(self):
        messagebus = Messagebus()

        assert messagebus.is_closed() is False
        await messagebus.close()
        assert messagebus.is_closed() is True

    async def test_get_context(self, handler_collection_factory):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})

        collection = handler_collection_factory()

        async def cmd1_handler(message, results, **kwargs):
            mb = get_running_messagebus()
            ctx = mb.get_context()
            _ = mb.handle_message(event1, test="test")
            results[message] = {
                "message": ctx["context_message"],
                "dependencies": kwargs,
            }

        collection.add_handler(cmd1, cmd1_handler)

        async def event1_handler(message, results, **kwargs):
            ctx = helpers.get_current_context()
            results[message] = {
                "message": ctx["context_message"],
                "dependencies": kwargs,
            }

        collection.add_handler(event1, event1_handler)

        messagebus = Messagebus()
        messagebus.include_collection(collection)
        await messagebus.run()

        assert messagebus.get_context() == {
            "context_message": None,
        }

        results1 = {}
        _ = messagebus.handle_message(cmd1, results=results1, arg1="abc", arg2=123)
        results2 = {}
        _ = messagebus.handle_message(cmd1, results=results2, arg3=789, arg4="xyz")

        assert results1 == results2 == {}
        await messagebus.stop()

        assert results1 == {
            cmd1: dict(
                message=cmd1,
                dependencies=dict(
                    arg1="abc",
                    arg2=123,
                ),
            ),
            event1: dict(
                message=event1, dependencies=dict(arg1="abc", arg2=123, test="test")
            ),
        }

        assert results2 == {
            cmd1: dict(message=cmd1, dependencies=dict(arg3=789, arg4="xyz")),
            event1: dict(
                message=event1, dependencies=dict(arg3=789, arg4="xyz", test="test")
            ),
        }

    async def test_fail_set_defaults_after_run_messagebus(
        self, handler_collection_factory
    ):
        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        await mb.run()

        with pytest.raises(
            RuntimeError, match="Can not set defaults to messagebus after run"
        ):
            mb.set_defaults("test", key=123)

        await mb.stop()
        await mb.close()

    @pytest.mark.parametrize(
        "defaults",
        (
            {"test-domain": dict(arg1="a", arg2=123)},
            {
                "test-domain-one": dict(arg1="a", arg2=123),
                "test-domain-two": dict(arg4="x", arg7=987),
            },
            {"test-domain": dict(test="xyz", t=123)},
        ),
    )
    async def test_set_defaults_to_included_collections(
        self, handler_collection_factory, defaults
    ):
        mb = Messagebus()
        collection1 = handler_collection_factory()
        collection2 = handler_collection_factory()
        mb.include_collection(collection1)
        mb.include_collection(collection2)

        for domain, defs in defaults.items():
            mb.set_defaults(domain, **defs)
        await mb.run()

        assert collection1.defaults == defaults
        assert collection2.defaults == defaults

    async def test_messagebus_lifespan_with_async_context_manager(
        self, handler_collection_factory
    ):
        results = []

        @asynccontextmanager
        async def lifespan(_md: IMessagebus):
            results.append("start")
            yield
            results.append("end")

        mb = Messagebus(lifespan=lifespan)
        mb.include_collection(handler_collection_factory())

        assert results == []

        await mb.run()
        assert results == ["start"]
        await mb.stop()
        assert results == ["start"]
        await mb.close()
        assert results == ["start", "end"]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning:d3m.*:")
    async def test_messagebus_lifespan_with_async_generator(
        self, handler_collection_factory
    ):
        results = []

        async def lifespan(_md: IMessagebus):
            results.append("start")
            yield
            results.append("end")

        mb = Messagebus(lifespan=lifespan)  # type: ignore[arg-type]
        mb.include_collection(handler_collection_factory())

        assert results == []

        await mb.run()
        assert results == ["start"]
        await mb.stop()
        assert results == ["start"]
        await mb.close()
        assert results == ["start", "end"]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning:d3m.*:")
    async def test_messagebus_lifespan_with_sync_generator(
        self, handler_collection_factory
    ):
        results = []

        def lifespan(_md: IMessagebus):
            results.append("start")
            yield
            results.append("end")

        mb = Messagebus(lifespan=lifespan)  # type: ignore[arg-type]
        mb.include_collection(handler_collection_factory())

        assert results == []

        await mb.run()
        assert results == ["start"]
        await mb.stop()
        assert results == ["start"]
        await mb.close()
        assert results == ["start", "end"]

    async def test_observe_messagebus(self, handler_collection_factory):
        events1 = []
        events2 = []

        async def func1(messagebus: IMessagebus, event: MessagebusEvents):
            events1.append((event, messagebus.is_running(), messagebus.is_closed()))

        async def func2(messagebus: IMessagebus, event: MessagebusEvents):
            events2.append((event, messagebus.is_running(), messagebus.is_closed()))

        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        mb.subscribe(
            MessagebusEvents.BEFORE_RUN,
            MessagebusEvents.AFTER_RUN,
            MessagebusEvents.BEFORE_STOP,
            MessagebusEvents.AFTER_STOP,
            MessagebusEvents.BEFORE_CLOSE,
            MessagebusEvents.AFTER_CLOSE,
            listener=func1,
        )
        mb.subscribe(
            MessagebusEvents.BEFORE_RUN,
            MessagebusEvents.AFTER_RUN,
            MessagebusEvents.BEFORE_STOP,
            MessagebusEvents.AFTER_STOP,
            MessagebusEvents.BEFORE_CLOSE,
            MessagebusEvents.AFTER_CLOSE,
            listener=func2,
        )

        await mb.run()
        assert events1 == [
            (MessagebusEvents.BEFORE_RUN, False, False),
            (MessagebusEvents.AFTER_RUN, True, False),
        ]
        assert events2 == [
            (MessagebusEvents.BEFORE_RUN, False, False),
            (MessagebusEvents.AFTER_RUN, True, False),
        ]

        events1.clear()
        events2.clear()

        await mb.stop()
        assert events1 == [
            (MessagebusEvents.BEFORE_STOP, True, False),
            (MessagebusEvents.AFTER_STOP, False, False),
        ]
        assert events2 == [
            (MessagebusEvents.BEFORE_STOP, True, False),
            (MessagebusEvents.AFTER_STOP, False, False),
        ]

        events1.clear()
        events2.clear()

        await mb.close()
        assert events1 == [
            (MessagebusEvents.BEFORE_CLOSE, False, True),
            (MessagebusEvents.AFTER_CLOSE, False, True),
        ]
        assert events2 == [
            (MessagebusEvents.BEFORE_CLOSE, False, True),
            (MessagebusEvents.AFTER_CLOSE, False, True),
        ]

    async def test_logging_listeners_errors(
        self, list_logger_handler, handler_collection_factory
    ):
        logs = list_logger_handler("d3m.messagebus")

        async def func(messagebus: IMessagebus, event: MessagebusEvents):
            1 / 0

        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        mb.subscribe(MessagebusEvents.BEFORE_RUN, listener=func)

        await mb.run()

        assert mb.is_running()
        assert len(logs) == 1

        log_record: logging.LogRecord = logs[0]

        assert log_record.name == "d3m.messagebus"
        assert (
            log_record.message
            == f"Fail exec {func} listener with MessagebusEvents.BEFORE_RUN event"
        )
        assert log_record.exc_info[0] == ZeroDivisionError
        assert log_record.levelname == "ERROR"

    async def test_unsubscribe(self, handler_collection_factory):
        events = []

        async def func(messagebus: IMessagebus, event: MessagebusEvents):
            events.append(event)

        mb = Messagebus()
        mb.include_collection(handler_collection_factory())
        mb.subscribe(
            MessagebusEvents.BEFORE_RUN, MessagebusEvents.AFTER_RUN, listener=func
        )

        await mb.run()

        assert events == [MessagebusEvents.BEFORE_RUN, MessagebusEvents.AFTER_RUN]

        await mb.stop()
        events.clear()

        mb.unsubscribe(MessagebusEvents.BEFORE_RUN, listener=func)
        await mb.run()

        assert events == [MessagebusEvents.AFTER_RUN]

    async def test_get_registered_commands(self, handler_collection_factory):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        cmd2 = UniversalMessage("test.TestCommand2", "COMMAND", {})
        collection1 = handler_collection_factory()
        collection2 = handler_collection_factory()

        async def test_handler1():
            pass

        async def test_handler2():
            pass

        collection1.add_handler(cmd1, test_handler1)
        collection2.add_handler(cmd2, test_handler2)

        mb = Messagebus()
        mb.include_collection(collection1)
        mb.include_collection(collection2)

        registered_commands = {
            cmd.__message_name__ for cmd in mb.get_registered_commands()
        }
        assert registered_commands == {"TestCommand1", "TestCommand2"}


class TestMessagebusPolicy:
    def test_new_messagebus(self):
        mbp = MessagebusPolicy()

        mb = mbp.new_messagebus()
        assert isinstance(mb, Messagebus)

        assert mb is not mbp.new_messagebus()

    @pytest.mark.parametrize("messagebus", (123, "abc", True, uuid4()))
    def test_fail_set_invalid_type_of_messagebus(self, messagebus):
        mbp = MessagebusPolicy()

        with pytest.raises(TypeError):
            mbp.set_messagebus(messagebus)  # noqa

        mbp.set_messagebus(None)

    def test_create_new_messagebus(self):
        mbp = MessagebusPolicy()
        mb = mbp.new_messagebus()
        assert type(mb) is Messagebus
        assert mbp._local.messagebus is None
        assert mbp._local.set_called is False
        assert mb is not mbp.new_messagebus()

    def test_set_messagebus(self):
        mbp = MessagebusPolicy()
        mb = Messagebus()
        mbp.set_messagebus(mb)
        assert mbp._local.messagebus is mb
        assert mbp._local.set_called is True

    def test_clean_messagebus(self):
        mbp = MessagebusPolicy()
        mbp.set_messagebus(mbp.new_messagebus())
        assert mbp._local.messagebus is not None
        assert mbp._local.set_called is True

        mbp.set_messagebus(None)
        assert mbp._local.messagebus is None
        assert mbp._local.set_called is True

    def test_get_new_instance_of_messagebus(self):
        mbp = MessagebusPolicy()
        mb = mbp.get_messagebus()

        assert isinstance(mb, Messagebus)
        assert mbp._local.messagebus is mb
        assert mbp._local.set_called is True

    def test_get_exists_instance_of_messagebus(self):
        mbp = MessagebusPolicy()
        mb = Messagebus()
        mbp.set_messagebus(mb)

        assert mb is mbp.get_messagebus()

    def test_fail_get_messagebus_in_sub_thread(self):
        result = queue.Queue()

        def _get_messagebus(policy: IMessagebusPolicy):
            try:
                messagebus = policy.get_messagebus()
                result.put(messagebus)
            except Exception as error:
                result.put(error)

        mbp = MessagebusPolicy()
        mb = mbp.get_messagebus()
        task = threading.Thread(target=_get_messagebus, args=(mbp,))
        task.start()
        task.join()

        err = result.get(timeout=1)
        assert isinstance(err, RuntimeError)
        assert err.args[0].startswith("There is not current messagebus in thread")
        assert mbp._local.messagebus is mb

    async def test_return_new_messagebus_if_current_messagebus_is_closed(
        self, handler_collection_factory
    ):
        mbp = MessagebusPolicy()
        mb = mbp.get_messagebus()
        mb.include_collection(handler_collection_factory())
        assert mb is mbp.get_messagebus()
        await mb.run()
        await mb.close()

        mb2 = mbp.get_messagebus()
        assert mb2 is not mb
