from typing import Generic, AsyncContextManager, Optional

from .abstractions import (
    TLock,
    TRepo,
    TLockKey,
    ILocker,
    IUnitOfWork,
    IRepository,
    IUnitOfWorkCtxMgr,
    IRepositoryBuilder,
    IUnitOfWorkBuilder,
)


__all__ = ["UnitOfWorkBuilder"]


class NullLocker(ILocker[TLock], Generic[TLock]):
    _instance: Optional["NullLocker"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __call__(self, __lock_key: TLockKey = None, /) -> AsyncContextManager[TLock]:
        return self

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class UnitOfWork(IUnitOfWork, Generic[TRepo]):
    def __init__(self, repository: IRepository):
        self._repository = repository

    @property
    def repository(self) -> TRepo:
        return self._repository  # type: ignore

    async def apply(self):
        await self._repository.commit()


class UnitOfWorkCtxMgr(IUnitOfWorkCtxMgr, Generic[TRepo, TLock]):
    def __init__(
        self,
        repository_builder: IRepositoryBuilder[TRepo],
        locker: AsyncContextManager[TLock],
    ):
        self._repository_builder = repository_builder
        self._locker_ctx_manager = locker
        self._lock: TLock | None = None
        self._uow: IUnitOfWork[TRepo] | None = None
        self._in_context = False

    async def __aenter__(self) -> IUnitOfWork[TRepo]:
        if self._in_context:
            raise RuntimeError("already enter to context")
        self._in_context = True
        self._lock = await self._locker_ctx_manager.__aenter__()
        repository = await self._repository_builder(self)
        if not isinstance(repository, IRepository):
            raise TypeError(
                f"{repository} returned from {self._repository_builder!r} "
                f"is not instance of d3m.uow.IRepository"
            )
        self._uow = self._get_uow(repository)
        return self._uow

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._in_context = False
        await self._locker_ctx_manager.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def lock(self) -> TLock | None:
        return self._lock

    @staticmethod
    def _get_uow(repository: IRepository) -> IUnitOfWork[TRepo]:
        return UnitOfWork(repository)


class UnitOfWorkBuilder(IUnitOfWorkBuilder, Generic[TRepo]):
    """
    Implementation of the IUnitOfWorkBuilder.

    This class is responsible for constructing and providing instances of Unit of Work context managers.

    Attributes:
        repository_builder (IRepositoryBuilder[TRepo]): An instance of IRepositoryBuilder[TRepo] that is used to construct repositories for the Unit of Work.
        locker (ILocker[TLock]): An instance of ILocker that is used to manage locks for the Unit of Work.

    Methods
        __call__: Returns a new instance of the Unit of Work context manager.

    Examples:
        >>> class ICustomRepository:
        ...     async def get_aggregate(self, reference):
        ...         ...
        >>> uow_builder = UnitOfWorkBuilder[ICustomRepository](RepositoryBuilder(), Locker())
        >>> async with uow_builder('lock-key') as uow:
        ...     aggregate = await uow.repository.get_aggregate(...)
        ...     ... # change state of the aggregate.
        ...     await uow.apply()

        >>> uow_builder = UnitOfWorkBuilder[ICustomRepository](RepositoryBuilder(), Locker())
        >>> uow_context_manager = uow_builder()
        >>> assert isinstance(uow_context_manager, IUnitOfWorkCtxMgr)
        >>> uow = await uow_context_manager.__aenter__()
        >>> assert isinstance(uow, IUnitOfWork)
    """

    def __init__(
        self,
        repository_builder: IRepositoryBuilder[TRepo],
        locker: ILocker | None = None,
    ):
        self._repository_builder = repository_builder
        self._locker = locker or NullLocker()

    def __call__(
        self, __lock_key: TLockKey = None, /
    ) -> IUnitOfWorkCtxMgr[TRepo, TLock]:
        """

        Args:
            __lock_key (`TLockKey`): The lock key used for locking the resources. Defaults to `None`.

        Returns:
            `IUnitOfWorkCtxMgr[TRepo, TLock]`: The context manager object that handles the unit of work.

        """
        return self._get_uow_context_manager(self._locker(__lock_key))

    def _get_uow_context_manager(self, locker: AsyncContextManager[TLock]):
        return UnitOfWorkCtxMgr(self._repository_builder, locker)
