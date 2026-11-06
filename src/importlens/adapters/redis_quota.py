from dataclasses import dataclass

from redis import Redis
from redis.exceptions import RedisError

from importlens.application.preview_quota import PreviewQuotaUnavailable, PreviewRateLimited

TAKE_PREVIEW_SLOT = """
local ttl = redis.call('PTTL', KEYS[1])
if ttl == -2 then
    redis.call('SET', KEYS[1], 1, 'PX', ARGV[2])
    return 0
end
if ttl == -1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    ttl = tonumber(ARGV[2])
end
if tonumber(redis.call('GET', KEYS[1])) >= tonumber(ARGV[1]) then
    return math.max(ttl, 1)
end
redis.call('INCR', KEYS[1])
return 0
"""


@dataclass(frozen=True, slots=True)
class RedisPreviewQuota:
    client: Redis
    key: str = "importlens:preview-quota:v1"
    limit: int = 12
    window_ms: int = 60_000

    def __post_init__(self) -> None:
        if self.limit < 1 or self.window_ms < 1:
            raise ValueError("Preview quota limit and window must be positive.")

    def take(self) -> None:
        try:
            wait_ms = self.client.eval(TAKE_PREVIEW_SLOT, 1, self.key, self.limit, self.window_ms)
        except RedisError as exc:
            raise PreviewQuotaUnavailable() from exc
        if not isinstance(wait_ms, int):
            raise PreviewQuotaUnavailable()
        if wait_ms > 0:
            raise PreviewRateLimited((wait_ms + 999) // 1000)
