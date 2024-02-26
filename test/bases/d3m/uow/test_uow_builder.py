from typing import AsyncContextManager
from uuid import uuid4, UUID

import pytest


from d3m.uow.abstractions import IUnitOfWorkCtxMgr, TLockKey, TLock
from d3m.uow import UnitOfWorkBuilder, ILocker, IRepositoryBuilder

from . import fakes as f


class TestUowBuilder:
    async def test_insert_to_storage(self):
        storage = f.Storage()

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(self, _uow: IUnitOfWorkCtxMgr) -> f.AbstractRepository:
                return f.FakeRepository(storage)

        builder = UnitOfWorkBuilder(RepoBuilder())

        async with builder() as uow:
            aggr = uow.repository.create("test")
            await uow.apply()

        assert isinstance(aggr, f.FakeAggregate)
        assert await storage.select() == (
            f.FakeRow(reference=aggr.reference, value="test"),
        )

    async def test_not_insert_to_storage_without_apply(self):
        storage = f.Storage()

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(self, _uow: IUnitOfWorkCtxMgr) -> f.AbstractRepository:
                return f.FakeRepository(storage)

        builder = UnitOfWorkBuilder(RepoBuilder())

        async with builder() as uow:
            aggr = uow.repository.create("test")

        assert isinstance(aggr, f.FakeAggregate)
        assert await storage.select() == ()

    async def test_get_from_storage(self):
        item_ref = uuid4()
        item_value = str(uuid4())
        storage = f.Storage(f.FakeRow(reference=item_ref, value=item_value))

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(self, _uow: IUnitOfWorkCtxMgr) -> f.AbstractRepository:
                return f.FakeRepository(storage)

        builder = UnitOfWorkBuilder(RepoBuilder())

        async with builder() as uow:
            aggr = await uow.repository.get(reference=item_ref)

        assert isinstance(aggr, f.FakeAggregate)
        assert aggr.reference == item_ref
        assert aggr._context.value == item_value

    async def test_update_in_storage(self):
        item_ref = uuid4()
        item_value = str(uuid4())
        storage = f.Storage(f.FakeRow(reference=item_ref, value=item_value))

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(self, _uow: IUnitOfWorkCtxMgr) -> f.AbstractRepository:
                return f.FakeRepository(storage)

        builder = UnitOfWorkBuilder(RepoBuilder())

        async with builder() as uow:
            aggr = await uow.repository.get(reference=item_ref)
            aggr.set_value("test")
            await uow.apply()

        row = await storage.select(reference=item_ref)
        assert row == (f.FakeRow(reference=item_ref, value="test"),)

    async def test_not_update_in_storage_without_apply(self):
        item_ref = uuid4()
        item_value = str(uuid4())
        storage = f.Storage(f.FakeRow(reference=item_ref, value=item_value))

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(self, _uow: IUnitOfWorkCtxMgr) -> f.AbstractRepository:
                return f.FakeRepository(storage)

        builder = UnitOfWorkBuilder(RepoBuilder())

        async with builder() as uow:
            aggr = await uow.repository.get(reference=item_ref)
            aggr.set_value("test")

        row = await storage.select(reference=item_ref)
        assert row == (f.FakeRow(reference=item_ref, value=item_value),)

    async def test_locker(self):
        lock_key = str(uuid4())
        lock_item = uuid4()
        locks = []

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(
                self, _uow: IUnitOfWorkCtxMgr[f.AbstractRepository, UUID]
            ) -> f.AbstractRepository:
                return f.FakeRepository(f.Storage())

        class Locker(ILocker[UUID]):
            lock_key: str

            def __call__(
                self, __lock_key: TLockKey = None, /
            ) -> AsyncContextManager[TLock]:
                self.lock_key = __lock_key
                locks.append("create context")
                return self

            async def __aenter__(self):
                locks.append("locked")
                return lock_item

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                locks.append("released")

        locker = Locker()

        builder = UnitOfWorkBuilder(RepoBuilder(), locker)

        assert locks == []

        assert hasattr(locker, "lock_key") is False

        uow_ctx = builder(lock_key)

        assert uow_ctx.lock is None
        assert locker.lock_key == lock_key
        assert locks == ["create context"]

        async with uow_ctx:
            assert uow_ctx.lock == lock_item
            assert locks == ["create context", "locked"]

        assert locks == ["create context", "locked", "released"]

    async def test_fail_enter_to_context_if_repository_build_return_not_valid_instance(
        self
    ):
        class RepoBuilder(IRepositoryBuilder[int]):
            async def __call__(
                self, _uow: IUnitOfWorkCtxMgr[int, UUID]
            ) -> f.AbstractRepository:
                return 1  # noqa

        builder = UnitOfWorkBuilder(RepoBuilder())
        uow_ctx = builder()
        with pytest.raises(
            TypeError,
            match=".* returned from .* is not instance of d3m.uow.IRepository",
        ):
            await uow_ctx.__aenter__()

    async def test_set_uow_context_manager_to_repo_builder(self):
        lock_item = uuid4()

        class Locker(ILocker[UUID]):
            def __call__(
                self, __lock_key: TLockKey = None, /
            ) -> AsyncContextManager[TLock]:
                return self

            async def __aenter__(self):
                return lock_item

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(
                self, _uow: IUnitOfWorkCtxMgr[f.AbstractRepository, UUID]
            ) -> f.AbstractRepository:
                assert isinstance(_uow, IUnitOfWorkCtxMgr)
                assert _uow.lock is lock_item
                return f.FakeRepository(f.Storage())

        builder = UnitOfWorkBuilder(RepoBuilder(), Locker())
        async with builder():
            pass

    async def test_fail_double_enter_to_context(self):
        class RepoBuilder(IRepositoryBuilder[f.AbstractRepository]):
            async def __call__(
                self, _uow: IUnitOfWorkCtxMgr[f.AbstractRepository, UUID]
            ) -> f.AbstractRepository:
                return f.FakeRepository(f.Storage())

        uow_ctx = UnitOfWorkBuilder(RepoBuilder())()

        async with uow_ctx:
            with pytest.raises(RuntimeError, match="already enter to context"):
                async with uow_ctx:
                    pass
