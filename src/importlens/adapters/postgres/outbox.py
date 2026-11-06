from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import UUID

from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from importlens.adapters.postgres.catalog import EventRow, PgConnection
from importlens.application.event_relay import CatalogMessage


@dataclass(frozen=True, slots=True)
class DeliveryRow(EventRow):
    stream_id: UUID


@dataclass(frozen=True, slots=True)
class PostgresDelivery:
    connection: PgConnection
    message: CatalogMessage

    def confirm(self) -> None:
        self.connection.execute(
            """
            UPDATE catalog_event_delivery SET last_published_revision = %s WHERE singleton
            """,
            (self.message.event.revision,),
        )


@dataclass(frozen=True, slots=True)
class PostgresOutbox:
    pool: ConnectionPool[PgConnection]

    @contextmanager
    def next_delivery(self) -> Iterator[PostgresDelivery | None]:
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=class_row(DeliveryRow)) as cursor:
                cursor.execute("""
                    SELECT e.revision, e.kind, e.previous_version_id, e.version_id, e.preview_id,
                           e.created_at, d.stream_id
                    FROM catalog_event_delivery d
                    JOIN catalog_events e ON e.revision = d.last_published_revision + 1
                    WHERE d.singleton
                    FOR UPDATE OF d SKIP LOCKED
                    """)
                row = cursor.fetchone()
            if row is None:
                yield None
            else:
                yield PostgresDelivery(connection, CatalogMessage(row.stream_id, row.to_domain()))
