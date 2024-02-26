from collections import namedtuple
from dataclasses import dataclass
from typing import NewType
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from d3m.core import DomainName, IEntity
from d3m.core.abstractions import IRootEntityMeta
from d3m.domain import Entity, RootEntity, DomainEvent
from d3m.core.abstractions import Version
from d3m.domain.entities import increment_version


class TestEntity:
    def test_use_reference_factory_entity_when_reference_not_set(self):
        class TEntity(Entity[UUID], reference_factory=uuid4):
            attr1: str
            attr2: int

        obj1 = TEntity(attr1="abc", attr2=123)
        obj2 = TEntity(attr1="abc", attr2=123)

        assert isinstance(obj1, IEntity)
        assert isinstance(obj1, BaseModel)
        assert isinstance(obj1.__reference__, UUID)
        assert obj1.__reference__ != obj2.__reference__

    def test_equal_entity(self):
        class TEntity(Entity[UUID]):
            attr1: str
            attr2: int

        obj1 = TEntity(attr1="abc", attr2=123)
        obj2 = TEntity(attr1="xyz", attr2=456, __reference__=obj1.__reference__)

        assert obj1 == obj2

    def test_namedtuple_as_reference(self):
        Reference = namedtuple("Reference", ["code", "version"])

        class TEntity(Entity[Reference]):
            pass

        entity = TEntity(__reference__=Reference("Test", 1))
        assert isinstance(entity.__reference__, Reference)
        assert entity.__reference__.code == "Test"
        assert entity.__reference__.version == 1

    def test_dataclass_as_reference(self):
        @dataclass(frozen=True)
        class Reference:
            code: str
            version: int

        class TEntity(Entity[Reference]):
            pass

        entity = TEntity(__reference__=Reference("Test", 1))

        assert entity.__reference__.code == "Test"
        assert entity.__reference__.version == 1

    def test_newtype_as_reference(self):
        Reference = NewType("Reference", UUID)

        class TEntity(Entity[Reference]):
            pass

        entity = TEntity()

        assert isinstance(entity.__reference__, UUID)

    def test_set_reference_from_init_parameters(self):
        class TEntity(Entity[UUID], reference_factory=uuid4):
            pass

        ref = uuid4()

        obj = TEntity(__reference__=ref)
        assert obj.__reference__ is ref

    def test_use_uuid4_factory_for_entity_annotated_by_uuid_reference(self):
        class TEntity(Entity[UUID]):
            pass

        obj = TEntity()

        assert isinstance(obj, IEntity)
        assert isinstance(obj, BaseModel)
        assert isinstance(obj.__reference__, UUID)

    def test_not_use_default_factory_for_not_uuid_reference(self):
        class TEntity(Entity[int]):
            pass

        with pytest.raises(TypeError):
            _ = TEntity()

        obj = TEntity(__reference__=1)
        assert obj.__reference__ == 1

    def test_inherit_reference_factory_from_parent_class(self):
        class TEntity1(Entity[UUID], reference_factory=lambda: UUID(int=1)):
            pass

        class TEntity2(TEntity1):
            pass

        assert TEntity2().__reference__ == UUID(int=1)

    def test_replace_parent_reference_factory(self):
        class TEntity1(Entity[UUID], reference_factory=lambda: UUID(int=1)):
            pass

        class TEntity2(TEntity1, reference_factory=lambda: UUID(int=2)):
            pass

        assert TEntity2().__reference__ == UUID(int=2)

    def test_not_annotated_entity_reference(self):
        class TEntity(Entity):
            pass

        obj1 = TEntity()

        assert isinstance(obj1, IEntity)
        assert isinstance(obj1, BaseModel)
        assert isinstance(obj1.__reference__, UUID)


class TestRootEntity:
    def test_domain_name(self):
        class TRE(RootEntity[UUID], domain="test"):
            pass

        assert isinstance(TRE, IRootEntityMeta)
        assert isinstance(TRE.__domain_name__, DomainName)
        assert TRE.__domain_name__ == "test"

        obj = TRE()
        assert isinstance(obj.__domain_name__, DomainName)
        assert obj.__domain_name__ == "test"
        assert obj.__version__ == 1
        assert isinstance(obj.__reference__, UUID)

    def test_fail_create_root_entity_without_set_domain_name(self):
        with pytest.raises(
            ValueError, match="required set domain name for root entity"
        ):

            class RE(RootEntity):
                pass

    def test_fail_replace_domain_name_in_inheritance_classes(self):
        class TRE1(RootEntity[UUID], domain="test"):
            pass

        with pytest.raises(RuntimeError) as exc:

            class TRE2(TRE1, domain="test2"):
                pass

        assert (
            str(exc.value)
            == "not allowed replace domain name in child class: domain.test_entities.TRE2"
        )

    def test_init_custom_version_and_reference(self):
        class TRE(RootEntity[UUID], domain="test"):
            pass

        ref = uuid4()

        obj = TRE(__reference__=ref, __version__=Version(2))

        assert obj.__version__ == 2
        assert obj.__reference__ is ref

    def test_add_event(self):
        class TRE(RootEntity[UUID], domain="test"):
            pass

        class TestEvent(DomainEvent, domain="test"):
            reference: UUID
            value: str

        obj = TRE()
        obj.create_event("TestEvent", reference=obj.__reference__, value="abc")
        obj.create_event("TestEvent", reference=obj.__reference__, value="abc")
        events = []
        for event in obj.collect_events():
            assert isinstance(event, DomainEvent)
            assert event.__domain_name__ == "test"
            assert event.__message_name__ == "TestEvent"
            assert event.__payload__ == {"reference": obj.__reference__, "value": "abc"}
            events.append(event)

        assert len(events) == 2

    def test_clear_events_after_collect(self):
        class TRE(RootEntity[UUID], domain="test"):
            pass

        class TestEvent(DomainEvent, domain="test"):
            reference: UUID
            value: str

        obj = TRE()
        obj.create_event("TestEvent", reference=obj.__reference__, value="abc")
        obj.create_event("TestEvent", reference=obj.__reference__, value="abc")
        events = list(obj.collect_events())
        assert len(events) == 2

        events = list(obj.collect_events())
        assert len(events) == 0

    def test_increment_version(self):
        class TRE(RootEntity[UUID], domain="test"):
            pass

        obj = TRE()

        assert obj.__version__ == 1

        increment_version(obj)
        assert obj.__version__ == 2

        increment_version(obj)
        assert obj.__version__ == 3
