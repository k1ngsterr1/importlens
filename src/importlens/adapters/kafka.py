import json
from datetime import UTC

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from importlens.application.event_relay import CatalogMessage, PublicationFailed
from importlens.domain.catalog import EventKind

CATALOG_EVENTS_TOPIC = "importlens.catalog-events.v1"
_DELIVERY_TIMEOUT_MS = 3_000
_EVENT_TYPES = {EventKind.APPLY: "catalog.applied", EventKind.ROLLBACK: "catalog.rolled_back"}


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str = CATALOG_EVENTS_TOPIC) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "importlens-relay",
                "enable.idempotence": True,
                "acks": "all",
                "allow.auto.create.topics": False,
                "message.timeout.ms": _DELIVERY_TIMEOUT_MS,
                "linger.ms": 0,
            }
        )

    def publish(self, message: CatalogMessage) -> None:
        acknowledged = False
        failure: KafkaError | None = None

        def on_delivery(error: KafkaError | None, _: Message) -> None:
            nonlocal acknowledged, failure
            acknowledged = error is None
            failure = error

        try:
            self._producer.produce(
                self._topic,
                key=str(message.stream_id),
                value=_encode(message),
                on_delivery=on_delivery,
            )
            self._producer.flush(_DELIVERY_TIMEOUT_MS / 1000 + 1)
        except (KafkaException, BufferError) as exc:
            raise PublicationFailed(f"{message.id}: {exc}") from exc
        if not acknowledged:
            reason = str(failure) if failure else "Kafka acknowledgement timed out."
            raise PublicationFailed(f"{message.id}: {reason}")


def _encode(message: CatalogMessage) -> bytes:
    event = message.event
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": message.id,
            "type": _EVENT_TYPES[event.kind],
            "catalog_id": str(message.stream_id),
            "revision": event.revision,
            "previous_version_id": event.previous_version_id,
            "version_id": event.version_id,
            "preview_id": str(event.preview_id) if event.preview_id is not None else None,
            "occurred_at": event.created_at.astimezone(UTC).isoformat(),
        },
        separators=(",", ":"),
    ).encode()
