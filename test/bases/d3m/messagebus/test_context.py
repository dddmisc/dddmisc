from contextvars import ContextVar
from types import MappingProxyType
from uuid import uuid4

import pytest
from d3m.core import UniversalMessage
from d3m.messagebus.context import MessagebusContext


class TestContext:
    def test_init_context(self):
        dependencies_ctx = ContextVar(str(uuid4()))
        context_message_ctx = ContextVar(str(uuid4()))
        ctx = MessagebusContext(
            dependencies_ctx=dependencies_ctx,
            context_message_ctx=context_message_ctx,
        )

        assert getattr(ctx, "_dependencies_ctx") is dependencies_ctx
        assert getattr(ctx, "_dependencies_value") == {}

        assert getattr(ctx, "_context_message_ctx") is context_message_ctx
        assert getattr(ctx, "_context_message") is None

        assert getattr(ctx, "_subcontext") is None

    def test_get_default_context_values(self):
        ctx = MessagebusContext()

        assert isinstance(ctx.dependencies, MappingProxyType)
        assert ctx.dependencies == {}
        assert ctx.context_message is None

    def test_get_current_dependencies(self):
        dependencies_ctx = ContextVar(str(uuid4()))
        dep_ctx = {
            "arg1": uuid4(),
            "arg2": uuid4(),
        }
        dependencies_ctx.set(dep_ctx)
        ctx = MessagebusContext(dependencies_ctx=dependencies_ctx)

        assert isinstance(ctx.dependencies, MappingProxyType)
        assert ctx.dependencies == dep_ctx

    def test_get_current_context_message(self):
        context_message_ctx = ContextVar(str(uuid4()))
        msg = UniversalMessage("test.Test", "COMMAND", {})
        context_message_ctx.set(msg)

        ctx = MessagebusContext(context_message_ctx=context_message_ctx)

        assert ctx.context_message is msg

    def test_update_dependencies(self):
        dependencies_ctx = ContextVar(str(uuid4()))
        context_message_ctx = ContextVar(str(uuid4()))

        dep_ctx = {
            "arg1": uuid4(),
            "arg2": uuid4(),
        }
        dependencies_ctx.set(dep_ctx)
        ctx = MessagebusContext(
            dependencies_ctx=dependencies_ctx, context_message_ctx=context_message_ctx
        )

        assert ctx.dependencies == dep_ctx

        ctx.update_dependencies({"arg3": 123})
        assert ctx.dependencies == {
            "arg1": dep_ctx["arg1"],
            "arg2": dep_ctx["arg2"],
            "arg3": 123,
        }

        ctx.update_dependencies({"arg1": "abc"})
        assert ctx.dependencies == {"arg1": "abc", "arg2": dep_ctx["arg2"], "arg3": 123}

    def test_set_context_message(self):
        dependencies_ctx = ContextVar(str(uuid4()))
        context_message_ctx = ContextVar(str(uuid4()))

        msg1 = UniversalMessage("test.Test", "COMMAND", {})
        msg2 = UniversalMessage("test.Test", "EVENT", {})

        context_message_ctx.set(msg1)
        ctx = MessagebusContext(
            dependencies_ctx=dependencies_ctx, context_message_ctx=context_message_ctx
        )

        assert ctx.context_message is msg1

        ctx.set_context_message(msg2)
        assert ctx.context_message is msg2

    def test_restore_context_after_exit(self):
        ctx = MessagebusContext()
        msg1 = UniversalMessage("test.Test1", "COMMAND", {})
        msg2 = UniversalMessage("test.Test2", "COMMAND", {})

        ctx.update_dependencies({"arg1": "abc"})
        ctx.set_context_message(msg1)

        with ctx:
            ctx.update_dependencies({"arg2": 123})
            ctx.set_context_message(msg2)

            assert ctx.dependencies == {"arg1": "abc", "arg2": 123}
            assert ctx.context_message is msg2

        assert ctx.dependencies == {"arg1": "abc"}
        assert ctx.context_message is msg1

    def test_init_subcontext(self):
        ctx = MessagebusContext()
        ctx.update_dependencies({"arg1": "abc"})
        ctx.set_context_message(UniversalMessage("test.Test", "COMMAND", {}))

        with ctx as sub_ctx:
            assert isinstance(sub_ctx, MessagebusContext)
            assert sub_ctx is not ctx
            assert ctx.dependencies == sub_ctx.dependencies
            assert ctx.context_message is sub_ctx.context_message
            assert getattr(ctx, "_subcontext") is sub_ctx

    def test_clear_subcontext_on_exit(self):
        ctx = MessagebusContext()
        msg1 = UniversalMessage("test.Test1", "COMMAND", {})
        msg2 = UniversalMessage("test.Test2", "COMMAND", {})
        msg3 = UniversalMessage("test.Test3", "COMMAND", {})
        ctx.update_dependencies({"arg1": "abc"})
        ctx.set_context_message(msg1)
        sub_ctx1 = ctx.__enter__()
        sub_ctx1.update_dependencies({"agr2": 123})
        sub_ctx1.set_context_message(msg2)

        sub_ctx2 = sub_ctx1.__enter__()
        sub_ctx2.update_dependencies({"agr3": True})
        sub_ctx2.set_context_message(msg3)

        assert (
            sub_ctx2.dependencies
            == sub_ctx1.dependencies
            == ctx.dependencies
            == {
                "arg1": "abc",
                "agr2": 123,
                "agr3": True,
            }
        )

        assert sub_ctx2.context_message is msg3

        ctx.__exit__(None, None, None)

        assert hasattr(sub_ctx1, "_dependencies_ctx") is False
        assert hasattr(sub_ctx1, "_context_message_ctx") is False
        assert hasattr(sub_ctx1, "_dependencies_value") is False
        assert hasattr(sub_ctx1, "_context_message") is False
        assert getattr(sub_ctx1, "_subcontext") is None
        assert hasattr(sub_ctx2, "_dependencies_ctx") is False
        assert hasattr(sub_ctx2, "_context_message_ctx") is False
        assert hasattr(sub_ctx2, "_dependencies_value") is False
        assert hasattr(sub_ctx2, "_context_message") is False

        assert ctx.dependencies == {"arg1": "abc"}
        assert ctx.context_message is msg1

    def test_fail_double_enter_to_context(self):
        ctx = MessagebusContext()

        with ctx:
            with pytest.raises(
                RuntimeError, match="Exit from current context before enter"
            ):
                with ctx:
                    pass
