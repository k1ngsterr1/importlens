from fastapi import HTTPException
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class BodyLimit:
    def __init__(self, app: ASGIApp, maximum: int) -> None:
        self.app = app
        self.maximum = maximum

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return

        lengths = [value for name, value in scope["headers"] if name == b"content-length"]
        if lengths and lengths[0].isdigit() and int(lengths[0]) > self.maximum:
            response = JSONResponse({"detail": "Request body is too large."}, status_code=413)
            await response(scope, receive, send)
            return

        size = 0

        async def limited_receive() -> Message:
            nonlocal size
            message = await receive()
            if message["type"] == "http.request":
                size += len(message.get("body", b""))
                if size > self.maximum:
                    raise HTTPException(413, "Request body is too large.")
            return message

        await self.app(scope, limited_receive, send)
