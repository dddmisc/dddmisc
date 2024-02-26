import datetime as dt
import warnings
from uuid import UUID
from d3m import core


class UniversalMessage(core.UniversalMessage):
    def __init__(
        self,
        full_message_name: str,
        message_type: core.MessageType | str,
        payload: dict,
        reference: UUID | None = None,
        timestamp: dt.datetime | None = None,
    ):  # pragma: no cover
        warnings.warn(
            "The `d3m.messagebus.UniversalMessage` class is deprecated; "
            "use `d3m.core.UniversalMessage` instead. Remove in next versions",
            DeprecationWarning,
        )
        super().__init__(full_message_name, message_type, payload, reference, timestamp)
