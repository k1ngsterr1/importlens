from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class ChangeKind(StrEnum):
    ADDED = "added"
    UPDATED = "updated"
    MISSING = "missing"


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


class IssueCode(StrEnum):
    HEADER = "header"
    ENCODING = "encoding"
    MALFORMED = "malformed"
    EMPTY = "empty"
    ROW_LIMIT = "row_limit"
    COLUMNS = "columns"
    SKU = "sku"
    NAME = "name"
    PRICE = "price"
    DUPLICATE = "duplicate"
    PRICE_JUMP = "price_jump"
    CATALOG_LIMIT = "catalog_limit"


class EventKind(StrEnum):
    APPLY = "apply"
    ROLLBACK = "rollback"


@dataclass(frozen=True, slots=True)
class Item:
    sku: str
    name: str
    price_cents: int


@dataclass(frozen=True, slots=True)
class FeedItem:
    item: Item
    line: int


@dataclass(frozen=True, slots=True)
class Issue:
    severity: Severity
    code: IssueCode
    message: str
    line: int | None = None
    sku: str | None = None


@dataclass(frozen=True, slots=True)
class Feed:
    rows: int
    items: tuple[FeedItem, ...]
    present: frozenset[str]
    issues: tuple[Issue, ...]
    complete: bool = True


@dataclass(frozen=True, slots=True)
class Change:
    kind: ChangeKind
    sku: str
    before: Item | None
    after: Item | None
    flagged: bool = False


@dataclass(frozen=True, slots=True)
class Summary:
    rows: int
    added: int
    updated: int
    missing: int
    unchanged: int
    errors: int
    warnings: int

    @property
    def changes(self) -> int:
        return self.added + self.updated + self.missing


@dataclass(frozen=True, slots=True)
class Comparison:
    summary: Summary
    changes: tuple[Change, ...]
    issues: tuple[Issue, ...]


@dataclass(frozen=True, slots=True)
class CatalogState:
    revision: int
    version_id: int
    item_count: int
    can_rollback: bool


@dataclass(frozen=True, slots=True)
class Preview:
    id: UUID
    filename: str
    base_revision: int
    summary: Summary
    created_at: datetime
    applied_revision: int | None = None


@dataclass(frozen=True, slots=True)
class CatalogEvent:
    revision: int
    kind: EventKind
    previous_version_id: int
    version_id: int
    preview_id: UUID | None
    created_at: datetime


def price_jump(before: Item, after: Item) -> bool:
    return abs(after.price_cents - before.price_cents) * 100 >= before.price_cents * 30


class PreviewStatus(StrEnum):
    READY = "ready"
    BLOCKED = "blocked"
    STALE = "stale"
    APPLIED = "applied"
    NO_CHANGES = "no_changes"


def preview_status(preview: Preview, current_revision: int) -> PreviewStatus:
    if preview.applied_revision is not None:
        return PreviewStatus.APPLIED
    if preview.base_revision != current_revision:
        return PreviewStatus.STALE
    if preview.summary.errors:
        return PreviewStatus.BLOCKED
    if not (preview.summary.added or preview.summary.updated):
        return PreviewStatus.NO_CHANGES
    return PreviewStatus.READY
