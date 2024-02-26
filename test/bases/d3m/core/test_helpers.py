import asyncio
import typing as t

import pytest
from d3m.core import (
    IHandlersCollection,
    IMessage,
    IMessagebus,
    IMessagebusPolicy,
    DomainName,
    MessagebusEvents,
    get_messagebus,
    get_messagebus_policy,
    get_running_messagebus,
    new_messagebus,
    set_messagebus,
    set_messagebus_policy,
    helpers,
)
from d3m.core.abstractions import Context
from pytest_mock import MockerFixture


class FakeMessagebus(IMessagebus):
    def subscribe(
        self,
        *events: MessagebusEvents,
        listener: t.Callable[[IMessagebus, MessagebusEvents], t.Coroutine],
    ):
        pass

    def unsubscribe(
        self,
        *events: MessagebusEvents,
        listener: t.Callable[[IMessagebus, MessagebusEvents], t.Coroutine],
    ):
        pass

    def get_context(self) -> Context:
        return Context(context_message=None, events_stream=None)

    async def run_until_complete(self, command: IMessage, **dependencies) -> t.Any:
        pass

    async def close(self):
        pass

    def is_running(self):
        pass

    def is_closed(self) -> bool:
        pass

    def include_collection(self, collection: IHandlersCollection):
        pass

    def handle_message(self, message: IMessage, **dependencies) -> asyncio.Future:
        pass

    _is_running = False
    _is_closed = False

    async def run(self):
        helpers.set_running_messagebus(self)

    async def stop(self):
        helpers.set_running_messagebus(None)

    def set_defaults(self, __domain: t.Union[DomainName, str], **defaults):
        pass


class FakeMessagebusPolicy(IMessagebusPolicy):
    def get_messagebus(self):
        pass

    def set_messagebus(self, messagebus: t.Optional[IMessagebus]):
        pass

    def new_messagebus(self) -> IMessagebus:
        pass


@pytest.fixture
def set_fake_messagebus_policy_as_default():
    dmbp = helpers.DEFAULT_MESSAGEBUS_POLICY
    mbp = helpers._messagebus_policy
    helpers.DEFAULT_MESSAGEBUS_POLICY = FakeMessagebusPolicy
    helpers._messagebus_policy = None
    yield
    helpers.DEFAULT_MESSAGEBUS_POLICY = dmbp
    helpers._messagebus_policy = mbp


class TestMessagebusPolicyFunctions:
    def test_set_messagebus_policy(self):
        assert helpers._messagebus_policy is None

        mbp = FakeMessagebusPolicy()
        set_messagebus_policy(mbp)
        assert helpers._messagebus_policy is mbp

        helpers._messagebus_policy = None  # Clear test artifacts

    def test_clear_messagebus_policy(self):
        mbp = FakeMessagebusPolicy()
        set_messagebus_policy(mbp)
        assert helpers._messagebus_policy is mbp

        set_messagebus_policy(None)

        assert helpers._messagebus_policy is None

    def test_fail_set_invalid_type_of_messagebus_policy(self):
        policy = 123
        with pytest.raises(
            TypeError,
            match="policy must be an instance of \"IMessagebusPolicy\" or None, not 'int'",
        ):
            set_messagebus_policy(policy)  # noqa

    def test_init_messagebus_policy(self, set_fake_messagebus_policy_as_default):
        assert helpers._messagebus_policy is None
        policy = get_messagebus_policy()
        assert isinstance(policy, IMessagebusPolicy)

    def test_get_exist_messagebus_policy(self, set_fake_messagebus_policy_as_default):
        policy = get_messagebus_policy()
        assert policy is get_messagebus_policy()


class TestMessagebusFunctions:
    def test_new_messagebus(self, mocker: MockerFixture):
        mbp = FakeMessagebusPolicy()
        spy = mocker.spy(mbp, "new_messagebus")
        set_messagebus_policy(mbp)
        new_messagebus()
        assert spy.called is True

        set_messagebus_policy(None)

    def test_set_messagebus(self, mocker: MockerFixture):
        mbp = FakeMessagebusPolicy()
        set_messagebus_policy(mbp)
        spy = mocker.spy(mbp, "set_messagebus")

        mb = FakeMessagebus()
        set_messagebus(mb)

        spy.assert_called_with(mb)

        set_messagebus_policy(None)

    def test_get_messagebus(self, mocker: MockerFixture):
        mbp = FakeMessagebusPolicy()
        set_messagebus_policy(mbp)
        spy = mocker.spy(mbp, "get_messagebus")

        get_messagebus()
        spy.assert_called()

        set_messagebus_policy(None)

    async def test_get_messagebus_return_running_messagebus(self):
        mbp = FakeMessagebusPolicy()
        set_messagebus_policy(mbp)
        mb = FakeMessagebus()
        await mb.run()

        new_mbp = FakeMessagebusPolicy()
        set_messagebus_policy(new_mbp)
        new_mb = get_messagebus()

        assert new_mb is mb
        await mb.stop()

        new_mb = get_messagebus()
        assert new_mb is not mb

        set_messagebus_policy(None)

    async def test_get_running_messagebus(self):
        mb1 = FakeMessagebus()
        await mb1.run()

        running_mb = get_running_messagebus()

        assert running_mb is mb1

        await running_mb.stop()

        set_messagebus_policy(None)

    def test_fail_get_running_messagebus_before_run(self):
        mbp = FakeMessagebusPolicy()
        set_messagebus_policy(mbp)

        with pytest.raises(RuntimeError, match="no running messagebus"):
            get_running_messagebus()

        set_messagebus_policy(None)

    async def test_fail_get_running_messagebus_after_stop(self):
        mb = FakeMessagebus()
        await mb.run()
        assert mb is get_running_messagebus()

        await mb.stop()

        with pytest.raises(RuntimeError, match="no running messagebus"):
            get_running_messagebus()

        set_messagebus_policy(None)

    async def test_fail_get_messagebus_without_policy(
        self, set_fake_messagebus_policy_as_default
    ):
        helpers.DEFAULT_MESSAGEBUS_POLICY = None
        helpers._messagebus_policy = None
        with pytest.raises(
            RuntimeError,
            match=("Required setup messagebus policy use 'set_messagebus_policy'"),
        ):
            helpers.get_messagebus()

    async def test_fail_get_context_without_running_messagebus(self):
        with pytest.raises(RuntimeError, match="no running messagebus"):
            helpers.get_current_context()

    async def test_get_empty_context(self, mocker: MockerFixture):
        mb = FakeMessagebus()
        try:
            await mb.run()

            assert helpers.get_current_context() == dict(
                context_message=None, events_stream=None
            )
        finally:
            await mb.stop()

    async def test_get_current_messagebus_context(self, mocker: MockerFixture):
        mb = FakeMessagebus()
        try:
            await mb.run()

            mocker.patch.object(
                mb, "get_context", return_value=Context(context_message="ABC")
            )

            assert helpers.get_current_context() == dict(
                context_message="ABC",
            )
        finally:
            await mb.stop()
