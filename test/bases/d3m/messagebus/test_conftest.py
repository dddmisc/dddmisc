import asyncio
from uuid import uuid4

from d3m.core import IMessage, UniversalMessage


class TestHandlerCollectionFactory:
    async def test_get_event_handlers(self, handler_collection_factory):
        collection = handler_collection_factory()
        results = []

        for i in range(10):

            async def handler(message: IMessage):
                results.append(message.__payload__["value"])

            collection.add_handler(UniversalMessage("test.Event", "EVENT", {}), handler)

        msg = UniversalMessage("test.Event", "EVENT", {"value": uuid4()})

        await asyncio.gather(*(h() for h in collection.get_event_handlers(msg)))

        assert len(results) == 10
        assert set(results) == {msg.__payload__["value"]}

        assert (
            collection.get_event_handlers(UniversalMessage("test.Event2", "EVENT", {}))
            == ()
        )

    async def test_get_command_handler(self, handler_collection_factory):
        collection = handler_collection_factory()

        for i in range(10):

            async def handler(message: IMessage):
                return i, message

            collection.add_handler(
                UniversalMessage("test.Event", "COMMAND", {}), handler
            )

        cmd = UniversalMessage("test.Event", "COMMAND", {})
        handler = collection.get_command_handler(cmd)

        assert await handler() == (9, cmd)

    async def test_get_command_handler_with_defaults(self, handler_collection_factory):
        collection = handler_collection_factory()

        async def handler(message: IMessage, i: int):
            return i, message

        collection.add_handler(UniversalMessage("test.Event", "COMMAND", {}), handler)
        collection.set_defaults("test", **{"i": 1})

        cmd = UniversalMessage("test.Event", "COMMAND", {})

        handler = collection.get_command_handler(cmd)
        assert await handler() == (1, cmd)

        handler = collection.get_command_handler(cmd, i=9)
        assert await handler() == (1, cmd)
