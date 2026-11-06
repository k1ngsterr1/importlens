from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from importlens.domain.catalog import CatalogEvent


@dataclass(frozen=True, slots=True)
class CatalogMessage:
    stream_id: UUID
    event: CatalogEvent

    @property
    def id(self) -> str:
        return f"{self.stream_id}:{self.event.revision}"


class PendingDelivery(Protocol):
    @property
    def message(self) -> CatalogMessage: ...

    def confirm(self) -> None: ...


class EventOutbox(Protocol):
    def next_delivery(self) -> AbstractContextManager[PendingDelivery | None]: ...


class EventPublisher(Protocol):
    def publish(self, message: CatalogMessage) -> None: ...


class PublicationFailed(Exception):
    pass


@dataclass(frozen=True, slots=True)
class EventRelay:
    outbox: EventOutbox
    publisher: EventPublisher

    def deliver_next(self) -> bool:
        with self.outbox.next_delivery() as delivery:
            if delivery is None:
                return False
            self.publisher.publish(delivery.message)
            delivery.confirm()
        return True
