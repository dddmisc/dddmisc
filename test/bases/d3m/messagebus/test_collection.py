import pytest
from d3m.core import IHandlersCollection, UniversalMessage
from d3m.messagebus.collection import MessagebusHandlersCollection


class TestMessagebusHandlersCollection:
    def test_init_collection(self):
        assert isinstance(MessagebusHandlersCollection(), IHandlersCollection)

    def test_len_collection(self, handler_collection_factory):
        mb_collection = MessagebusHandlersCollection()
        collections = []

        for i in range(1, 100):
            collections.append(handler_collection_factory())

        for i, collection in enumerate(collections):
            mb_collection.include_collection(collection)
            assert len(mb_collection) == i + 1

    def test_include_collection(self, handler_collection_factory):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})
        cmd2 = UniversalMessage("test.TestCommand2", "COMMAND", {})
        event2 = UniversalMessage("test.TestEvent2", "EVENT", {})

        async def handler(message, **kwargs):
            return message

        collection1 = handler_collection_factory()

        collection1.add_handler(cmd1, handler)
        collection1.add_handler(event1, handler)

        collection2 = handler_collection_factory()

        collection2.add_handler(cmd2, handler)
        collection2.add_handler(event2, handler)

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)

        assert mb_collection.get_command_handler(cmd1) is not None
        assert len(mb_collection.get_event_handlers(event1)) == 1
        assert len(mb_collection.get_event_handlers(event2)) == 0
        with pytest.raises(RuntimeError):
            mb_collection.get_command_handler(cmd2)

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)
        mb_collection.include_collection(collection2)

        assert mb_collection.get_command_handler(cmd1) is not None
        assert mb_collection.get_command_handler(cmd2) is not None
        assert len(mb_collection.get_event_handlers(event1)) == 1
        assert len(mb_collection.get_event_handlers(event2)) == 1

    def test_double_include_same_collection(self, handler_collection_factory):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})

        async def handler(message, **kwargs):
            return message

        collection1 = handler_collection_factory()

        collection1.add_handler(cmd1, handler)
        collection1.add_handler(event1, handler)

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)

        assert mb_collection.get_command_handler(cmd1) is not None
        assert len(mb_collection.get_event_handlers(event1)) == 1

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)
        mb_collection.include_collection(collection1)

        assert mb_collection.get_command_handler(cmd1) is not None
        assert len(mb_collection.get_event_handlers(event1)) == 1

        assert len(mb_collection) == 1

    def test_aggregate_event_handlers_from_different_collection(
        self, handler_collection_factory
    ):
        event1 = UniversalMessage("test.TestEvent1", "EVENT", {})

        async def handler(message, **kwargs):
            return message

        collection1 = handler_collection_factory()
        collection1.add_handler(event1, handler)

        collection2 = handler_collection_factory()
        collection2.add_handler(event1, handler)

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)

        assert len(mb_collection.get_event_handlers(event1)) == 1

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)
        mb_collection.include_collection(collection2)

        assert len(mb_collection.get_event_handlers(event1)) == 2

    def test_fail_get_command_handler_from_different_collection(
        self, handler_collection_factory
    ):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})

        async def handler(message, **kwargs):
            return message

        collection1 = handler_collection_factory()
        collection1.add_handler(cmd1, handler)

        collection2 = handler_collection_factory()
        collection2.add_handler(cmd1, handler)

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)

        assert mb_collection.get_command_handler(cmd1) is not None

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)
        mb_collection.include_collection(collection2)

        with pytest.raises(
            RuntimeError,
            match='More one handler registered for command "test.TestCommand1"',
        ):
            mb_collection.get_command_handler(cmd1)

    @pytest.mark.parametrize("collection", (123, "abc", True, None))
    def test_fail_include_invalid_type_of_collection(self, collection):
        mb_collection = MessagebusHandlersCollection()
        with pytest.raises(
            TypeError,
            match=f"Invalid collection type expected type "
            f"{IHandlersCollection!r} got {collection!r}",
        ):
            mb_collection.include_collection(collection)

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
    def test_set_defaults_for_included_collections(
        self, handler_collection_factory, defaults
    ):
        mb_collection = MessagebusHandlersCollection()
        for domain, _defaults in defaults.items():
            mb_collection.set_defaults(domain, **_defaults)

        collection1 = handler_collection_factory()
        collection2 = handler_collection_factory()

        mb_collection.include_collection(collection1)
        mb_collection.include_collection(collection2)
        mb_collection.update_defaults()
        collections = list(mb_collection._collections)
        assert collections[0].defaults == defaults
        assert collections[1].defaults == defaults

    def test_get_registered_commands(self, handler_collection_factory):
        cmd1 = UniversalMessage("test.TestCommand1", "COMMAND", {})
        cmd2 = UniversalMessage("test.TestCommand2", "COMMAND", {})
        cmd3 = UniversalMessage("test.TestCommand3", "COMMAND", {})

        async def handler1():
            pass

        async def handler2():
            pass

        async def handler3():
            pass

        collection1 = handler_collection_factory()
        collection1.add_handler(cmd1, handler1)
        collection1.add_handler(cmd2, handler2)

        collection2 = handler_collection_factory()
        collection2.add_handler(cmd3, handler3)

        mb_collection = MessagebusHandlersCollection()
        mb_collection.include_collection(collection1)
        mb_collection.include_collection(collection2)

        handlers = set(
            cmd.__message_name__ for cmd in mb_collection.get_registered_commands()
        )
        assert handlers == {"TestCommand1", "TestCommand2", "TestCommand3"}
