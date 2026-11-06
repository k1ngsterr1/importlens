from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, File, Query, UploadFile
from pydantic import BaseModel, Field

from importlens.domain.catalog import CatalogEvent, CatalogState, Change, ChangeKind, Issue, Preview
from importlens.web.dependencies import ImportService, upload_preview
from importlens.web.preview_route import PreviewRoute

preview_router = APIRouter(route_class=PreviewRoute)
router = APIRouter(prefix="/api/v1")
PageSize = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0, le=1_000_000)]


class RollbackRequest(BaseModel):
    expected_revision: int = Field(strict=True, ge=0, le=2**63 - 2)


@router.get("/catalog")
def catalog(imports: ImportService) -> CatalogState:
    return imports.catalog.state()


@router.get("/catalog/history")
def history(imports: ImportService, limit: PageSize = 25, offset: Offset = 0) -> list[CatalogEvent]:
    return list(imports.catalog.history(limit, offset))


@router.post("/catalog/rollback")
def rollback(command: RollbackRequest, imports: ImportService) -> CatalogEvent:
    return imports.rollback(command.expected_revision)


@preview_router.post("/previews", status_code=201)
def create_preview(file: Annotated[UploadFile, File()], imports: ImportService) -> Preview:
    return upload_preview(imports, file)


@router.get("/previews/{preview_id}")
def preview(preview_id: UUID, imports: ImportService) -> Preview:
    return imports.catalog.preview(preview_id)


@router.get("/previews/{preview_id}/changes")
def changes(
    preview_id: UUID,
    imports: ImportService,
    kind: ChangeKind | None = None,
    limit: PageSize = 25,
    offset: Offset = 0,
) -> list[Change]:
    imports.catalog.preview(preview_id)
    return list(imports.catalog.changes(preview_id, limit, offset, kind))


@router.get("/previews/{preview_id}/issues")
def issues(
    preview_id: UUID, imports: ImportService, limit: PageSize = 25, offset: Offset = 0
) -> list[Issue]:
    imports.catalog.preview(preview_id)
    return list(imports.catalog.issues(preview_id, limit, offset))


@router.post("/previews/{preview_id}/apply")
def apply(preview_id: UUID, imports: ImportService) -> CatalogEvent:
    return imports.apply(preview_id)


router.include_router(preview_router)
