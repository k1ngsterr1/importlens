from typing import Annotated, cast

from fastapi import Depends, HTTPException, Request, UploadFile

from importlens.application.imports import Imports
from importlens.domain.catalog import Preview


def get_imports(request: Request) -> Imports:
    return cast(Imports, request.app.state.imports)


ImportService = Annotated[Imports, Depends(get_imports)]


def upload_preview(imports: Imports, file: UploadFile) -> Preview:
    return imports.create_preview(file.filename or "supplier.csv", file.file)


def require_same_origin(request: Request) -> None:
    if request.method != "POST":
        return
    origin = request.headers.get("origin")
    if origin is not None and origin != f"{request.url.scheme}://{request.url.netloc}":
        raise HTTPException(403, "Cross-origin writes are forbidden.")
