from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from psycopg import Connection, Cursor
from psycopg.rows import class_row, scalar_row, tuple_row
from psycopg_pool import ConnectionPool

from importlens.domain.catalog import (
    CatalogEvent,
    CatalogState,
    Change,
    ChangeKind,
    Comparison,
    EventKind,
    Issue,
    IssueCode,
    Item,
    Preview,
    Severity,
    Summary,
)
from importlens.domain.errors import PreviewNotFound

type PgConnection = Connection[tuple[object, ...]]
type PgCursor = Cursor[tuple[object, ...]]


@dataclass(frozen=True, slots=True)
class PreviewRow:
    id: UUID
    filename: str
    base_revision: int
    rows: int
    added: int
    updated: int
    missing: int
    unchanged: int
    errors: int
    warnings: int
    created_at: datetime
    applied_revision: int | None

    def to_domain(self) -> Preview:
        summary = Summary(
            self.rows,
            self.added,
            self.updated,
            self.missing,
            self.unchanged,
            self.errors,
            self.warnings,
        )
        return Preview(
            self.id,
            self.filename,
            self.base_revision,
            summary,
            self.created_at,
            self.applied_revision,
        )


@dataclass(frozen=True, slots=True)
class ChangeRow:
    kind: str
    sku: str
    old_name: str | None
    old_price: int | None
    new_name: str | None
    new_price: int | None
    flagged: bool

    def to_domain(self) -> Change:
        return Change(
            ChangeKind(self.kind),
            self.sku,
            _item(self.sku, self.old_name, self.old_price),
            _item(self.sku, self.new_name, self.new_price),
            self.flagged,
        )


@dataclass(frozen=True, slots=True)
class IssueRow:
    severity: str
    code: str
    message: str
    line: int | None
    sku: str | None

    def to_domain(self) -> Issue:
        return Issue(
            Severity(self.severity), IssueCode(self.code), self.message, self.line, self.sku
        )


@dataclass(frozen=True, slots=True)
class EventRow:
    revision: int
    kind: str
    previous_version_id: int
    version_id: int
    preview_id: UUID | None
    created_at: datetime

    def to_domain(self) -> CatalogEvent:
        return CatalogEvent(
            self.revision,
            EventKind(self.kind),
            self.previous_version_id,
            self.version_id,
            self.preview_id,
            self.created_at,
        )


@dataclass(frozen=True, slots=True)
class Head:
    revision: int
    version_id: int


class PostgresCatalog:
    def __init__(self, pool: ConnectionPool[PgConnection]) -> None:
        self.pool = pool

    def state(self) -> CatalogState:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=class_row(CatalogState)) as cursor,
        ):
            cursor.execute("""
                SELECT s.revision, s.version_id, v.item_count,
                       COALESCE(e.kind = 'apply', false) AS can_rollback
                FROM catalog_state s
                JOIN catalog_versions v ON v.id = s.version_id
                LEFT JOIN catalog_events e ON e.revision = s.revision
                """)
            result = cursor.fetchone()
            assert result is not None
            return result

    def snapshot(self, version_id: int) -> list[Item]:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=class_row(Item)) as cursor,
        ):
            cursor.execute(
                "SELECT sku, name, price_cents FROM catalog_items WHERE version_id = %s",
                (version_id,),
            )
            return cursor.fetchall()

    def save_preview(
        self, preview_id: UUID, filename: str, base_revision: int, comparison: Comparison
    ) -> Preview:
        s = comparison.summary
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=tuple_row) as cursor,
        ):
            cursor.execute(
                """
                INSERT INTO previews
                    (id, filename, base_revision, rows, added, updated, missing,
                     unchanged, errors, warnings)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    preview_id,
                    filename,
                    base_revision,
                    s.rows,
                    s.added,
                    s.updated,
                    s.missing,
                    s.unchanged,
                    s.errors,
                    s.warnings,
                ),
            )
            _copy_preview(cursor, preview_id, comparison)
            return _preview(connection, preview_id)

    def preview(self, preview_id: UUID) -> Preview:
        with self.pool.connection() as connection:
            return _preview(connection, preview_id)

    def changes(
        self, preview_id: UUID, limit: int, offset: int, kind: ChangeKind | None
    ) -> list[Change]:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=class_row(ChangeRow)) as cursor,
        ):
            if kind is None:
                cursor.execute(
                    """
                    SELECT kind, sku, old_name, old_price, new_name, new_price, flagged
                    FROM preview_changes WHERE preview_id = %s ORDER BY position LIMIT %s OFFSET %s
                    """,
                    (preview_id, limit, offset),
                )
            else:
                cursor.execute(
                    """
                    SELECT kind, sku, old_name, old_price, new_name, new_price, flagged
                    FROM preview_changes WHERE preview_id = %s AND kind = %s
                    ORDER BY position LIMIT %s OFFSET %s
                    """,
                    (preview_id, kind, limit, offset),
                )
            return [row.to_domain() for row in cursor]

    def issues(self, preview_id: UUID, limit: int, offset: int) -> list[Issue]:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=class_row(IssueRow)) as cursor,
        ):
            cursor.execute(
                """
                SELECT severity, code, message, line, sku FROM preview_issues
                WHERE preview_id = %s ORDER BY position LIMIT %s OFFSET %s
                """,
                (preview_id, limit, offset),
            )
            return [row.to_domain() for row in cursor]

    def history(self, limit: int, offset: int) -> list[CatalogEvent]:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=class_row(EventRow)) as cursor,
        ):
            cursor.execute(
                "SELECT * FROM catalog_events ORDER BY revision DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            return [row.to_domain() for row in cursor]

    @contextmanager
    def edit(self) -> Iterator[PostgresCatalogEdit]:
        with self.pool.connection() as connection:
            yield PostgresCatalogEdit(connection, _lock_head(connection))


@dataclass(frozen=True, slots=True)
class PostgresCatalogEdit:
    connection: PgConnection
    head: Head

    @property
    def revision(self) -> int:
        return self.head.revision

    def preview(self, preview_id: UUID) -> Preview:
        return _preview(self.connection, preview_id)

    def event(self, revision: int) -> CatalogEvent | None:
        return _event(self.connection, revision)

    def materialize(self, preview: Preview) -> int:
        return _materialize(self.connection, self.head, preview)

    def advance(self, kind: EventKind, version_id: int, preview_id: UUID | None) -> CatalogEvent:
        return _advance(self.connection, self.head, kind, version_id, preview_id)


def _item(sku: str, name: str | None, price: int | None) -> Item | None:
    return Item(sku, name, price) if name is not None and price is not None else None


def _preview(connection: PgConnection, preview_id: UUID) -> Preview:
    with connection.cursor(row_factory=class_row(PreviewRow)) as cursor:
        cursor.execute(
            """
            SELECT p.*, e.revision AS applied_revision
            FROM previews p LEFT JOIN catalog_events e ON e.preview_id = p.id
            WHERE p.id = %s
            """,
            (preview_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise PreviewNotFound("Preview not found.")
    return row.to_domain()


def _copy_preview(cursor: PgCursor, preview_id: UUID, comparison: Comparison) -> None:
    with cursor.copy("COPY preview_changes FROM STDIN") as copy:
        for position, change in enumerate(comparison.changes):
            before, after = change.before, change.after
            copy.write_row(
                (
                    preview_id,
                    position,
                    change.kind,
                    change.sku,
                    before.name if before else None,
                    before.price_cents if before else None,
                    after.name if after else None,
                    after.price_cents if after else None,
                    change.flagged,
                )
            )

    with cursor.copy("COPY preview_issues FROM STDIN") as copy:
        for position, issue in enumerate(comparison.issues):
            copy.write_row(
                (
                    preview_id,
                    position,
                    issue.severity,
                    issue.code,
                    issue.message,
                    issue.line,
                    issue.sku,
                )
            )


def _lock_head(connection: PgConnection) -> Head:
    with connection.cursor(row_factory=class_row(Head)) as cursor:
        cursor.execute("""
            SELECT revision, version_id FROM catalog_state FOR UPDATE
            """)
        result = cursor.fetchone()
        assert result is not None
        return result


def _event(connection: PgConnection, revision: int) -> CatalogEvent | None:
    with connection.cursor(row_factory=class_row(EventRow)) as cursor:
        cursor.execute("SELECT * FROM catalog_events WHERE revision = %s", (revision,))
        row = cursor.fetchone()
        return row.to_domain() if row is not None else None


def _materialize(connection: PgConnection, head: Head, preview: Preview) -> int:
    with connection.cursor(row_factory=scalar_row) as cursor:
        cursor.execute(
            """
            INSERT INTO catalog_versions (item_count)
            SELECT item_count + %s FROM catalog_versions WHERE id = %s RETURNING id
            """,
            (preview.summary.added, head.version_id),
        )
        version_id: int | None = cursor.fetchone()
        assert version_id is not None
    connection.execute(
        """
        INSERT INTO catalog_items (version_id, sku, name, price_cents)
        SELECT %s, old.sku, old.name, old.price_cents
        FROM catalog_items old
        WHERE old.version_id = %s AND NOT EXISTS (
            SELECT 1 FROM preview_changes c
            WHERE c.preview_id = %s AND c.sku = old.sku AND c.kind = 'updated'
        )
        UNION ALL
        SELECT %s, sku, new_name, new_price FROM preview_changes
        WHERE preview_id = %s AND kind IN ('added', 'updated')
        """,
        (version_id, head.version_id, preview.id, version_id, preview.id),
    )
    return version_id


def _advance(
    connection: PgConnection, head: Head, kind: EventKind, version_id: int, preview_id: UUID | None
) -> CatalogEvent:
    revision = head.revision + 1
    connection.execute(
        "UPDATE catalog_state SET revision = %s, version_id = %s WHERE singleton",
        (revision, version_id),
    )
    with connection.cursor(row_factory=class_row(EventRow)) as cursor:
        cursor.execute(
            """
            INSERT INTO catalog_events (revision, kind, previous_version_id, version_id, preview_id)
            VALUES (%s, %s, %s, %s, %s) RETURNING *
            """,
            (revision, kind, head.version_id, version_id, preview_id),
        )
        row = cursor.fetchone()
        assert row is not None
        return row.to_domain()
