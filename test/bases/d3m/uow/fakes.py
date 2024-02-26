# domain
import abc
import asyncio
from typing import TypedDict
from uuid import uuid4, UUID

from pydantic import BaseModel, Field

from d3m.uow import IRepository


class FakeRootEntity(BaseModel):
    reference: UUID = Field(default_factory=uuid4)
    value: str

    def __hash__(self):
        return hash(self.reference)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.reference == other.reference


class AbstractAggregate(abc.ABC):
    @abc.abstractmethod
    def set_value(self, value: str):
        pass

    @property
    @abc.abstractmethod
    def reference(self) -> UUID:
        pass


class FakeAggregate(AbstractAggregate):
    def __init__(self, context: FakeRootEntity):
        self._context = context

    def set_value(self, value: str):
        self._context.value = value

    @property
    def reference(self) -> UUID:
        return self._context.reference


# storage


class FakeRow(TypedDict):
    reference: UUID
    value: str


class Storage:
    def __init__(self, *rows: FakeRow):
        self._storage: list[FakeRow] = list(rows)

    async def insert(self, reference: UUID, value: str):
        await asyncio.sleep(0.001)
        record = await self._get_row(reference)
        await asyncio.sleep(0.001)
        if record is not None:
            raise RuntimeError("reference conflict")
        await asyncio.sleep(0.001)
        self._storage.append(FakeRow(reference=reference, value=value))

    async def update(self, reference: UUID, value: str):
        await asyncio.sleep(0.001)
        record = await self._get_row(reference)
        await asyncio.sleep(0.001)
        if record is None:
            raise RuntimeError("reference not found")
        await asyncio.sleep(0.001)
        record["value"] = value

    async def delete(self, reference: UUID):
        record = await self._get_row(reference)
        await asyncio.sleep(0.001)
        if record is None:
            raise RuntimeError("reference not found")
        await asyncio.sleep(0.001)
        self._storage.remove(record)

    async def select(self, reference: UUID = None, value: str = None) -> tuple[FakeRow]:
        await asyncio.sleep(0.001)
        if reference is None and value is None:
            await asyncio.sleep(0.001)
            return tuple(self._storage)
        else:
            await asyncio.sleep(0.001)
            return tuple(
                FakeRow(**row)
                for row in self._storage
                if (reference == row["reference"] or value == row["value"])
            )

    async def _get_row(self, reference: UUID):
        await asyncio.sleep(0.001)
        return next(
            (row for row in self._storage if row["reference"] == reference), None
        )


# Repository


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    async def get(self, reference: UUID) -> FakeAggregate:
        pass

    @abc.abstractmethod
    def create(self, value: str) -> FakeAggregate:
        pass


class FakeRepository(AbstractRepository, IRepository):
    def __init__(self, engine: Storage):
        self._engine = engine
        self._insert_seen: set[FakeRootEntity] = set()
        self._update_seen: set[FakeRootEntity] = set()

    def create(self, value: str) -> FakeAggregate:
        entity = FakeRootEntity(value=value)
        self._insert_seen.add(entity)
        return FakeAggregate(entity)

    async def get(self, reference: UUID) -> FakeAggregate:
        rows = await self._engine.select(reference=reference)
        if len(rows) == 0:
            raise RuntimeError("object not found")
        elif len(rows) > 1:
            raise RuntimeError("multiple objects selected")
        entity = FakeRootEntity.model_validate(rows[0])
        self._update_seen.add(entity)
        return FakeAggregate(entity)

    async def commit(self):
        for entity in self._insert_seen:
            await self._engine.insert(entity.reference, entity.value)

        for entity in self._update_seen:
            await self._engine.update(entity.reference, entity.value)
