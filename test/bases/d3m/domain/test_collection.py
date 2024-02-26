from uuid import uuid4

import pytest

from d3m.domain.collection import DomainObjectsCollection


class TestObjectsCollection:
    def test_singleton(self):
        assert DomainObjectsCollection() is DomainObjectsCollection()

    def test_register_command(self):
        class Command1:
            pass

        class Command2:
            pass

        DomainObjectsCollection().register(
            category="command",
            domain="test",
            name="Command1",
            klass=Command1,
        )

        DomainObjectsCollection().register(
            category="COMMAND",
            domain="test",
            name="Command2",
            klass=Command2,
        )

        test_commands = DomainObjectsCollection().get_domain_objects(
            category="command",
            domain="test",
        )
        assert test_commands == (Command1, Command2)

        test_commands = DomainObjectsCollection().get_domain_objects(
            category="command",
            domain="test-two",
        )
        assert test_commands == ()

        command = DomainObjectsCollection().get_domain_object(
            category="command", domain="test", name="Command1"
        )
        assert command is Command1

        command = DomainObjectsCollection().get_domain_object(
            category="command", domain="test", name="Command2"
        )
        assert command is Command2

    def test_idempotent_register_the_same_object(self):
        class Command1:
            pass

        DomainObjectsCollection().register(
            category="command",
            domain="test",
            name="Command1",
            klass=Command1,
        )
        DomainObjectsCollection().register(
            category="command",
            domain="test",
            name="Command1",
            klass=Command1,
        )

        test_commands = DomainObjectsCollection().get_domain_objects(
            category="command",
            domain="test",
        )
        assert test_commands == (Command1,)

        command = DomainObjectsCollection().get_domain_object(
            category="command", domain="test", name="Command1"
        )
        assert command is Command1

    def test_fail_register_other_class_as_already_registered_class(self):
        class Command1:
            pass

        class Command2:
            pass

        DomainObjectsCollection().register(
            category="command",
            domain="test",
            name="Command",
            klass=Command1,
        )

        with pytest.raises(
            RuntimeError,
            match='Other command class for "test" domain with name "Command" already registered',
        ):
            DomainObjectsCollection().register(
                category="command",
                domain="test",
                name="Command",
                klass=Command2,
            )

    def test_fail_register_class_registered_as_other_object(self):
        class Command1:
            pass

        DomainObjectsCollection().register(
            category="command",
            domain="test",
            name="Command",
            klass=Command1,
        )

        with pytest.raises(
            RuntimeError,
            match=f"{Command1!r} already registered in collection with other name",
        ):
            DomainObjectsCollection().register(
                category="other_category",
                domain="test",
                name="Command",
                klass=Command1,
            )

    def test_fail_get_unregistered_domain_object(self):
        with pytest.raises(
            RuntimeError,
            match='command class for "test" domain with name "Test" not registered',
        ):
            DomainObjectsCollection().get_domain_object("command", "test", "Test")

    def test_return_empty_sequence_objects_for_not_registered_domain_or_object_type(
        self
    ):
        assert DomainObjectsCollection().get_domain_objects(str(uuid4()), "test") == ()
