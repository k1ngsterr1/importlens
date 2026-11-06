from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from importlens.demo import DemoFeed, demo_upload
from importlens.domain.catalog import Change, ChangeKind, Issue, preview_status
from importlens.web.dependencies import ImportService, upload_preview
from importlens.web.preview_route import PreviewRoute

preview_router = APIRouter(route_class=PreviewRoute)
router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=Path(__file__).with_name("templates"))
PAGE_SIZE = 20


def dollars(cents: int) -> str:
    return f"${cents // 100:,}.{cents % 100:02d}"


templates.env.filters["dollars"] = dollars
templates.env.filters["count"] = lambda value: format(value, ",")


@router.get("/", response_class=HTMLResponse)
def workspace(
    request: Request,
    imports: ImportService,
    preview_id: UUID | None = None,
    tab: Literal["changes", "issues"] = "changes",
    kind: ChangeKind | None = None,
    page: Annotated[int, Query(ge=1, le=50_000)] = 1,
) -> Response:
    state = imports.catalog.state()
    preview = imports.catalog.preview(preview_id) if preview_id is not None else None
    status = preview_status(preview, state.revision) if preview is not None else None
    rows: list[Change] = []
    issues: list[Issue] = []
    total = 0
    if preview is not None:
        summary = preview.summary
        if tab == "issues":
            total = summary.errors + summary.warnings
        else:
            totals = {
                ChangeKind.ADDED: summary.added,
                ChangeKind.UPDATED: summary.updated,
                ChangeKind.MISSING: summary.missing,
                None: summary.changes,
            }
            total = totals[kind]

        page = min(page, max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE))
        offset = (page - 1) * PAGE_SIZE
        if tab == "issues":
            issues = list(imports.catalog.issues(preview.id, PAGE_SIZE, offset))
        else:
            rows = list(imports.catalog.changes(preview.id, PAGE_SIZE, offset, kind))

    return templates.TemplateResponse(
        request,
        "workspace.html",
        {
            "state": state,
            "preview": preview,
            "status": status,
            "tab": tab,
            "kind": kind,
            "rows": rows,
            "issues": issues,
            "page": page,
            "page_size": PAGE_SIZE,
            "total": total,
            "history": imports.catalog.history(6, 0),
        },
        headers={"Cache-Control": "no-store"},
    )


@preview_router.post("/upload")
def upload(file: Annotated[UploadFile, File()], imports: ImportService) -> RedirectResponse:
    preview = upload_preview(imports, file)
    return _open_preview(preview.id)


@preview_router.post("/samples/{variant}")
def sample(variant: Literal["clean", "with-errors"], imports: ImportService) -> RedirectResponse:
    filename, source = demo_upload(DemoFeed(variant))
    preview = imports.create_preview(filename, source)
    return _open_preview(preview.id)


@router.get("/samples/{variant}.csv")
def download(variant: DemoFeed) -> Response:
    filename, source = demo_upload(variant)
    return Response(
        source.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/review/{preview_id}/apply")
def apply(preview_id: UUID, imports: ImportService) -> RedirectResponse:
    imports.apply(preview_id)
    return _open_preview(preview_id)


@router.post("/rollback")
def rollback(
    revision: Annotated[int, Form(ge=0, le=2**63 - 2)], imports: ImportService
) -> RedirectResponse:
    imports.rollback(revision)
    return RedirectResponse("/", status_code=303)


def _open_preview(preview_id: UUID) -> RedirectResponse:
    return RedirectResponse(f"/?preview_id={preview_id}", status_code=303)


router.include_router(preview_router)
