from collections.abc import Sequence
from contextlib import AbstractContextManager
from typing import BinaryIO, Protocol
from uuid import UUID

from importlens.domain.catalog import (
    CatalogEvent,
    CatalogState,
    Change,
    ChangeKind,
    Comparison,
    EventKind,
    Feed,
    Issue,
    Item,
    Preview,
)


class FeedReader(Protocol):
    def read(self, source: BinaryIO) -> Feed: ...


class ChangeDetector(Protocol):
    def changed_skus(self, catalog: Sequence[Item], incoming: Sequence[Item]) -> Sequence[str]: ...


class CatalogEdit(Protocol):
    @property
    def revision(self) -> int: ...

    def preview(self, preview_id: UUID) -> Preview: ...

    def event(self, revision: int) -> CatalogEvent | None: ...

    def materialize(self, preview: Preview) -> int: ...

    def advance(
        self, kind: EventKind, version_id: int, preview_id: UUID | None
    ) -> CatalogEvent: ...


class CatalogStore(Protocol):
    def state(self) -> CatalogState: ...

    def snapshot(self, version_id: int) -> Sequence[Item]: ...

    def save_preview(
        self, preview_id: UUID, filename: str, base_revision: int, comparison: Comparison
    ) -> Preview: ...

    def preview(self, preview_id: UUID) -> Preview: ...

    def changes(
        self, preview_id: UUID, limit: int, offset: int, kind: ChangeKind | None
    ) -> Sequence[Change]: ...

    def issues(self, preview_id: UUID, limit: int, offset: int) -> Sequence[Issue]: ...

    def history(self, limit: int, offset: int) -> Sequence[CatalogEvent]: ...

    def edit(self) -> AbstractContextManager[CatalogEdit]: ...
