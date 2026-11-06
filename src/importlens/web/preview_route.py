from collections.abc import Callable, Coroutine
from typing import Any, cast

from fastapi import Request
from fastapi.routing import APIRoute
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response

from importlens.application.preview_quota import PreviewQuota
from importlens.web.dependencies import require_same_origin


class PreviewRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handle = super().get_route_handler()

        async def preview(request: Request) -> Response:
            require_same_origin(request)
            quota = cast(PreviewQuota | None, request.app.state.preview_quota)
            if quota is not None:
                await run_in_threadpool(quota.take)
            return await handle(request)

        return preview
