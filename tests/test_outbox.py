from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from threading import Event

import psycopg
import pytest

from importlens.adapters.postgres.migrate import MIGRATIONS, migrate
from importlens.adapters.postgres.outbox import PostgresOutbox
from importlens.application.event_relay import CatalogMessage, EventRelay, PublicationFailed
from importlens.application.imports import Imports
from importlens.bootstrap import build_imports, connection_pool
from importlens.domain.catalog import CatalogEvent, EventKind


@dataclass
class Recorder:
    messages: list[CatalogMessage] = field(default_factory=list)

    def publish(self, message: CatalogMessage) -> None:
        self.messages.append(message)


@dataclass
class Unavailable:
    attempted: CatalogMessage | None = None

    def publish(self, message: CatalogMessage) -> None:
        self.attempted = message
        raise PublicationFailed("Broker unavailable")


def apply_product(imports: Imports, price: int = 10) -> CatalogEvent:
    source = BytesIO(f"sku,name,price\nA,Alpha,{price}\n".encode())
    preview = imports.create_preview("supplier.csv", source)
    return imports.apply(preview.id)


def test_upgrade_replays_history(
    empty_database: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = MIGRATIONS / "001_catalog.sql"
    (tmp_path / initial.name).write_bytes(initial.read_bytes())
    with monkeypatch.context() as patch:
        patch.setattr("importlens.adapters.postgres.migrate.MIGRATIONS", tmp_path)
        migrate(empty_database)
    with connection_pool(empty_database) as pool:
        imports = build_imports(pool)
        applied = apply_product(imports)
        rollback = imports.rollback(applied.revision)

    migrate(empty_database)
    publisher = Recorder()
    with connection_pool(empty_database) as pool:
        relay = EventRelay(PostgresOutbox(pool), publisher)
        assert relay.deliver_next()
        migrate(empty_database)
        assert relay.deliver_next()
        assert not relay.deliver_next()
    assert [message.event for message in publisher.messages] == [applied, rollback]


def test_retry_after_failure_and_checkpoint_rollback(database: str) -> None:
    with connection_pool(database) as pool:
        imports = build_imports(pool)
        applied = apply_product(imports)
        rollback = imports.rollback(applied.revision)
        outbox = PostgresOutbox(pool)
        unavailable = Unavailable()
        with pytest.raises(PublicationFailed):
            EventRelay(outbox, unavailable).deliver_next()
        assert imports.catalog.state().revision == rollback.revision

        first = Recorder()
        with psycopg.connect(database, autocommit=True) as connection:
            connection.execute("""
                ALTER TABLE catalog_event_delivery ADD CONSTRAINT prevent_checkpoint
                    CHECK (last_published_revision = 0) NOT VALID
                """)
            with pytest.raises(psycopg.errors.CheckViolation):
                EventRelay(outbox, first).deliver_next()
            connection.execute(
                "ALTER TABLE catalog_event_delivery DROP CONSTRAINT prevent_checkpoint"
            )

        resumed = Recorder()
        relay = EventRelay(outbox, resumed)
        assert relay.deliver_next()
        assert relay.deliver_next()
        assert not relay.deliver_next()

    assert unavailable.attempted == first.messages[0] == resumed.messages[0]
    assert [message.event.kind for message in resumed.messages] == [
        EventKind.APPLY,
        EventKind.ROLLBACK,
    ]
    assert [message.event.revision for message in resumed.messages] == [1, 2]


def test_parallel_relay_does_not_lock_catalog(database: str) -> None:
    publishing = Event()
    release = Event()

    class PausedPublisher(Recorder):
        def publish(self, message: CatalogMessage) -> None:
            publishing.set()
            assert release.wait(5)
            super().publish(message)

    with connection_pool(database) as pool:
        imports = build_imports(pool)
        first = apply_product(imports)
        publisher = PausedPublisher()
        other = Recorder()
        with ThreadPoolExecutor(max_workers=1) as executor:
            job = executor.submit(EventRelay(PostgresOutbox(pool), publisher).deliver_next)
            try:
                assert publishing.wait(5)
                second = apply_product(imports, price=20)
                relay = EventRelay(PostgresOutbox(pool), other)
                assert not relay.deliver_next()
            finally:
                release.set()
            assert job.result(timeout=5)

        assert relay.deliver_next()
        assert not relay.deliver_next()
    assert [message.event for message in publisher.messages] == [first]
    assert [message.event for message in other.messages] == [second]
