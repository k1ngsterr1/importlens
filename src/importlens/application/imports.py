from dataclasses import dataclass, field
from threading import BoundedSemaphore
from typing import BinaryIO
from uuid import UUID, uuid4

from importlens.application.ports import CatalogStore, ChangeDetector, FeedReader
from importlens.domain.catalog import (
    CatalogEvent,
    EventKind,
    Preview,
    PreviewStatus,
    preview_status,
)
from importlens.domain.errors import (
    ImportCapacityReached,
    NothingToApply,
    PreviewBlocked,
    RollbackUnavailable,
    StaleRevision,
    UploadTooLarge,
)
from importlens.domain.review import review

MAX_UPLOAD_BYTES = 24 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Imports:
    catalog: CatalogStore
    reader: FeedReader
    detector: ChangeDetector
    _preview_slots: BoundedSemaphore = field(
        default_factory=lambda: BoundedSemaphore(2), init=False, repr=False, compare=False
    )

    def create_preview(self, filename: str, source: BinaryIO) -> Preview:
        if not self._preview_slots.acquire(blocking=False):
            raise ImportCapacityReached("Two previews are processing. Try again shortly.")
        try:
            return self._create_preview(filename, source)
        finally:
            self._preview_slots.release()

    def _create_preview(self, filename: str, source: BinaryIO) -> Preview:
        source.seek(0, 2)
        if source.tell() > MAX_UPLOAD_BYTES:
            raise UploadTooLarge("CSV files can be at most 24 MiB.")
        source.seek(0)
        feed = self.reader.read(source)
        base = self.catalog.state()
        current = self.catalog.snapshot(base.version_id) if feed.complete else ()
        incoming = [entry.item for entry in feed.items]
        different = self.detector.changed_skus(current, incoming) if incoming else ()
        comparison = review(current, feed, different)
        basename = filename.replace("\\", "/").rsplit("/", 1)[-1]
        label = "".join(char for char in basename if char.isprintable())[:255]
        return self.catalog.save_preview(
            uuid4(), label or "supplier.csv", base.revision, comparison
        )

    def apply(self, preview_id: UUID) -> CatalogEvent:
        with self.catalog.edit() as edit:
            preview = edit.preview(preview_id)
            status = preview_status(preview, edit.revision)
            match status:
                case PreviewStatus.APPLIED:
                    assert preview.applied_revision is not None
                    event = edit.event(preview.applied_revision)
                    assert event is not None
                    return event
                case PreviewStatus.STALE:
                    raise StaleRevision("The catalog changed. Create a fresh preview.")
                case PreviewStatus.BLOCKED:
                    raise PreviewBlocked("Fix the CSV errors before applying.")
                case PreviewStatus.NO_CHANGES:
                    raise NothingToApply("This preview has no catalog changes to apply.")
                case PreviewStatus.READY:
                    version_id = edit.materialize(preview)
                    return edit.advance(EventKind.APPLY, version_id, preview.id)

    def rollback(self, expected_revision: int) -> CatalogEvent:
        with self.catalog.edit() as edit:
            if edit.revision != expected_revision:
                previous_result = edit.event(expected_revision + 1)
                if previous_result is not None and previous_result.kind == EventKind.ROLLBACK:
                    return previous_result
                raise StaleRevision("The catalog changed. Refresh before rolling back.")

            active = edit.event(edit.revision)
            if active is None or active.kind != EventKind.APPLY:
                raise RollbackUnavailable("There is no applied import to roll back.")
            return edit.advance(EventKind.ROLLBACK, active.previous_version_id, None)
