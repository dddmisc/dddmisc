import os
import threading

from . import DomainName, MessageName
from .abstractions import IMessagebus, IMessagebusPolicy, Context

# Messagebus policy.  The policy itself is always global, even if the
# policy's rules say that there is a messagebus per thread (or other
# notion of context).  The default policy is installed by the first
# call to get_messagebus_policy().
_messagebus_policy: IMessagebusPolicy | None = None

# Lock for protecting the on-the-fly creation of the messagebus policy.
_lock = threading.Lock()

DEFAULT_MESSAGEBUS_POLICY: type[IMessagebusPolicy] | None = None


def _init_messagebus_policy():
    global _messagebus_policy
    with _lock:
        if _messagebus_policy is None and DEFAULT_MESSAGEBUS_POLICY is not None:
            _messagebus_policy = DEFAULT_MESSAGEBUS_POLICY()
        else:
            raise RuntimeError(
                "Required setup messagebus policy use 'set_messagebus_policy'"
            )


def get_messagebus_policy() -> IMessagebusPolicy:
    """
    Retrieves the messagebus policy.

    If the message bus policy is not initialized, it will be initialized first.

    Returns:
        IMessagebusPolicy: The messagebus policy.

    """
    if _messagebus_policy is None:
        _init_messagebus_policy()
    return _messagebus_policy  # type: ignore


def set_messagebus_policy(policy: IMessagebusPolicy | None):
    """Set the messagebus policy.

    Args:
        policy (IMessagebusPolicy | None): If `None`, no policy will be set. Otherwise, the specified `policy` will be set as the messagebus policy.

    Raises:
        TypeError: If `policy` is not an instance of `IMessagebusPolicy` or `None`.
    """
    global _messagebus_policy
    if policy is not None and not isinstance(policy, IMessagebusPolicy):
        raise TypeError(
            f'policy must be an instance of "IMessagebusPolicy" or None, '
            f"not '{type(policy).__name__}'"
        )
    _messagebus_policy = policy


class _RunningMessagebus(threading.local):
    mb_pid: tuple[IMessagebus | None, int | None] = (None, None)


_running_messagebus = _RunningMessagebus()


def get_running_messagebus() -> IMessagebus:
    """
    Returns the running instance of the messagebus.

    Returns:
        IMessagebus: The running instance of the messagebus.

    Raises:
        RuntimeError: If there is no running messagebus instance.
    """
    messagebus = _get_running_messagebus()
    if messagebus is None:
        raise RuntimeError("no running messagebus")
    return messagebus


def _get_running_messagebus() -> IMessagebus | None:
    running_messagebus, pid = _running_messagebus.mb_pid
    if running_messagebus is not None and pid == os.getpid():
        return running_messagebus
    return None


def set_running_messagebus(messagebus: IMessagebus | None):
    _running_messagebus.mb_pid = (messagebus, os.getpid())


def get_messagebus() -> IMessagebus:
    """
    Returns the current IMessagebus instance.

    If there is a running IMessagebus instance, it returns that instance.
    Otherwise, it retrieves the IMessagebus instance using the get_messagebus method from the message bus policy.

    Returns:
        IMessagebus: The current IMessagebus instance.

    """
    current_messagebus = _get_running_messagebus()
    if current_messagebus is not None:
        return current_messagebus
    return get_messagebus_policy().get_messagebus()


def set_messagebus(messagebus: IMessagebus):
    """
    Set the messagebus for the messagebus policy.

    Args:
        messagebus (IMessagebus): The message bus object to set.
    """
    get_messagebus_policy().set_messagebus(messagebus)


def new_messagebus():
    """
    Returns a new instance of the MessageBus class.

    This method retrieves the message bus policy using the get_messagebus_policy() function, and
    calls the new_messagebus() method of the policy to create a new message bus instance.

    Returns:
        IMessagebus: A new instance of the MessageBus class.

    """
    return get_messagebus_policy().new_messagebus()


def get_current_context() -> Context:
    mb = get_running_messagebus()
    return mb.get_context()


def parse_full_message_name(full_message_name) -> tuple[DomainName, MessageName]:
    domain_name, message_name = full_message_name.rsplit(".", 1)
    return DomainName(domain_name), MessageName(message_name)
