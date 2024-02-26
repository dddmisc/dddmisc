import abc
from typing import TypeVar, Generic, TypeAlias, AsyncContextManager


TRepo = TypeVar("TRepo")
TLock = TypeVar("TLock")
TLockKey: TypeAlias = str | None


class IUnitOfWork(Generic[TRepo], abc.ABC):
    """
    An interface representing a unit of work for a repository.

    Attributes:
        TRepo: A generic type representing the repository.
        repository: Returns the repository instance.

    Methods:
        apply: Commits all changes in the repository.

    """

    @property
    @abc.abstractmethod
    def repository(self) -> TRepo:
        """
        Returns:
            Repository instance
        """

    @abc.abstractmethod
    async def apply(self):
        """
        Commit all changes in repository
        """


class IUnitOfWorkCtxMgr(Generic[TRepo, TLock], abc.ABC):
    """
    Abstract base class for context managers implementing unit of work pattern.

    This class allows for the creation of context managers that encapsulate a unit of work,
    providing methods for entering and exiting the context, as well as accessing a lock object.

    Attributes:
        TLock: A generic type representing the lock object
        TRepo: A generic type representing the repository
        lock (TLock): Returns the lock object returned from the locker.

    Methods:
        __aenter__: Called when entering the context. Returns an instance of IUnitOfWork.
        __aexit__: Called when exiting the context.
    """

    @abc.abstractmethod
    async def __aenter__(self) -> IUnitOfWork[TRepo]:
        pass

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    @abc.abstractmethod
    def lock(self) -> TLock:
        """
        Returns:
            Lock object returned from locker
        """


class IUnitOfWorkBuilder(Generic[TRepo], abc.ABC):
    """
    Build unit of work context manager with an optional lock key.

    Args:
        lock_key: The lock key used for locking the unit of work.

    Returns:
        The unit of work context manager.

    """

    @abc.abstractmethod
    def __call__(self, lock_key: str | None = None) -> IUnitOfWorkCtxMgr[TRepo, TLock]:
        """
        Build unit of work context manager and with lock used lock key

        Args:
            lock_key: lock key used for lock

        Returns:
            (IUnitOfWorkCtxMgr[TRepo, TLock]): Unit of work context manager
        """


class IRepository(abc.ABC):
    """
    An abstract base class for repositories.

    Methods:
        commit: Commits the current state of the repository to the persistence storage.

    """

    @abc.abstractmethod
    async def commit(self) -> None:
        """
        Commit all changes to storage
        """


class IRepositoryBuilder(Generic[TRepo], abc.ABC):
    """
    Interface for repository builder using by unit of work context manager.

    Attributes:
        TRepo: interface of repository to be built.

    """

    @abc.abstractmethod
    async def __call__(
        self, __uow_context_manager: IUnitOfWorkCtxMgr, /
    ) -> IRepository | TRepo:
        """
        Build repository instance use unit of work context manager.

        Args:
            __uow_context_manager (IUnitOfWorkCtxMgr): unit of work context manager.

        Returns:
            repository class
        """


class ILocker(Generic[TLock], abc.ABC):
    """
    A generic abstract class representing a locker.
    This class defines the interface for creating a context manager that uses a lock key.

    Attributes:
        TLock: generic type, returned on entering to async context manager building by locker instance.

    Methods:
        __call__: Create a context manager that creates a lock on entering the context
            and releases it on exiting the context.

    Examples:
        To create a custom locker, subclass ILocker and implement the __call__ method.
        >>> class RedLocker(ILocker):
        >>>     def __call__(self, __lock_key: TLockKey = None, /) -> AsyncContextManager[TLock]:
        >>>         # implementation goes here
    """

    @abc.abstractmethod
    def __call__(self, __lock_key: TLockKey = None, /) -> AsyncContextManager[TLock]:
        """
        Create context manager make lock use lock key

        Args:
            __lock_key (str | None): Optional lock key

        Returns:
            Async context manager create lock object on enter to context and release lock on exit from context
        """
