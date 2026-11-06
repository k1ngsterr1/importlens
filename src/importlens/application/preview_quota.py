from typing import Protocol

from importlens.domain.errors import ImportLensError


class PreviewQuota(Protocol):
    def take(self) -> None: ...


class PreviewRateLimited(ImportLensError):
    code = "preview_rate_limited"

    def __init__(self, retry_after: int) -> None:
        super().__init__(f"The preview limit was reached. Try again in {retry_after}s.")
        self.retry_after = retry_after


class PreviewQuotaUnavailable(ImportLensError):
    code = "preview_quota_unavailable"

    def __init__(self) -> None:
        super().__init__("New previews are temporarily unavailable. Try again shortly.")
