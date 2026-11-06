from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, nullcontext
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from importlens.application.imports import MAX_UPLOAD_BYTES
from importlens.application.preview_quota import (
    PreviewQuota,
    PreviewQuotaUnavailable,
    PreviewRateLimited,
)
from importlens.bootstrap import (
    build_imports,
    configured_preview_quota,
    connection_pool,
    database_url,
)
from importlens.domain.errors import (
    ImportCapacityReached,
    ImportLensError,
    PreviewNotFound,
    UploadTooLarge,
)
from importlens.web import api, console
from importlens.web.body_limit import BodyLimit
from importlens.web.dependencies import require_same_origin


def create_app(url: str | None = None, *, preview_quota: PreviewQuota | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = connection_pool(url or database_url())
        try:
            await run_in_threadpool(pool.open, wait=True)
            app.state.imports = build_imports(pool)
            quota_context = (
                nullcontext(preview_quota)
                if preview_quota is not None
                else configured_preview_quota()
            )
            with quota_context as quota:
                app.state.preview_quota = quota
                yield
        finally:
            await run_in_threadpool(pool.close)

    app = FastAPI(
        title="ImportLens",
        version="0.1.0",
        lifespan=lifespan,
        dependencies=[Depends(require_same_origin)],
    )
    app.add_middleware(BodyLimit, maximum=MAX_UPLOAD_BYTES + 64 * 1024)
    app.mount("/static", StaticFiles(directory=Path(__file__).with_name("static")), name="static")
    app.include_router(api.router)
    app.include_router(console.router)
    app.add_exception_handler(ImportLensError, import_error)
    return app


async def import_error(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, ImportLensError)
    status = 409
    headers: dict[str, str] = {}
    if isinstance(exc, PreviewNotFound):
        status = 404
    elif isinstance(exc, UploadTooLarge):
        status = 413
    elif isinstance(exc, PreviewRateLimited):
        status = 429
        headers["Retry-After"] = str(exc.retry_after)
    elif isinstance(exc, (ImportCapacityReached, PreviewQuotaUnavailable)):
        status = 503

    if request.url.path.startswith("/api/"):
        return JSONResponse(
            {"error": {"code": exc.code, "message": str(exc)}},
            status_code=status,
            headers=headers,
        )
    return console.templates.TemplateResponse(
        request,
        "error.html",
        {"message": str(exc), "status": status},
        status_code=status,
        headers=headers,
    )


app = create_app()
