import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from socket import socket
from time import monotonic, sleep
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from importlens.adapters.redis_quota import RedisPreviewQuota
from importlens.application.preview_quota import PreviewRateLimited
from importlens.web.main import create_app

UPLOAD = {"file": ("vendor.csv", b"sku,name,price\nA,Alpha,10\n", "text/csv")}


@pytest.fixture
def redis_url() -> str:
    url = os.environ.get("IMPORTLENS_TEST_REDIS_URL")
    if url is None:
        pytest.skip("Set IMPORTLENS_TEST_REDIS_URL for Redis quota integration tests.")
    return url


@pytest.fixture
def quota_key(redis_url: str) -> Iterator[str]:
    key = f"importlens:test:quota:{uuid4()}"
    try:
        yield key
    finally:
        with Redis.from_url(redis_url) as connection:
            connection.delete(key)


def test_parallel_redis_clients_share_quota(redis_url: str, quota_key: str) -> None:
    with Redis.from_url(redis_url) as first, Redis.from_url(redis_url) as second:
        quotas = [RedisPreviewQuota(client, quota_key, limit=4) for client in (first, second)]

        def take(index: int) -> int:
            try:
                quotas[index % len(quotas)].take()
            except PreviewRateLimited as exc:
                return exc.retry_after
            return 0

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(take, range(16)))
        assert results.count(0) == 4
        assert all(0 <= delay <= 60 for delay in results)
        assert first.get(quota_key) == b"4"
        assert 0 < first.pttl(quota_key) <= 60_000


def test_redis_quota_resets_after_expiry(redis_url: str, quota_key: str) -> None:
    with Redis.from_url(redis_url) as connection:
        quota = RedisPreviewQuota(connection, quota_key, limit=1, window_ms=500)
        quota.take()
        before = connection.pttl(quota_key)
        with pytest.raises(PreviewRateLimited, match="Try again in 1s"):
            quota.take()
        assert 0 < connection.pttl(quota_key) <= before

        deadline = monotonic() + 2
        while connection.exists(quota_key) and monotonic() < deadline:
            sleep(0.02)
        assert not connection.exists(quota_key)
        quota.take()
        assert connection.get(quota_key) == b"1"


@pytest.mark.parametrize("initial_count", [0, 1])
def test_redis_quota_repairs_missing_expiry(
    redis_url: str, quota_key: str, initial_count: int
) -> None:
    with Redis.from_url(redis_url) as connection:
        connection.set(quota_key, initial_count)
        quota = RedisPreviewQuota(connection, quota_key, limit=1)
        if initial_count:
            with pytest.raises(PreviewRateLimited) as rejection:
                quota.take()
            assert rejection.value.retry_after == 60
        else:
            quota.take()
        assert connection.get(quota_key) == b"1"
        assert 0 < connection.pttl(quota_key) <= 60_000


def test_shared_http_quota_blocks_before_reading_body(
    database: str, redis_url: str, quota_key: str
) -> None:
    with Redis.from_url(redis_url) as first, Redis.from_url(redis_url) as second:
        first_app = create_app(database, preview_quota=RedisPreviewQuota(first, quota_key, limit=3))
        second_app = create_app(
            database, preview_quota=RedisPreviewQuota(second, quota_key, limit=3)
        )
        with TestClient(first_app) as api, TestClient(second_app) as console:
            forbidden = console.post("/samples/clean", headers={"Origin": "https://other.example"})
            assert forbidden.status_code == 403
            assert first.get(quota_key) is None

            preview = api.post("/api/v1/previews", files=UPLOAD)
            assert preview.status_code == 201, preview.text
            assert console.post("/upload", files=UPLOAD, follow_redirects=False).status_code == 303
            assert console.post("/samples/clean", follow_redirects=False).status_code == 303

            def unread() -> Iterator[bytes]:
                pytest.fail("Rate-limited multipart body was read.")
                yield b""

            blocked = api.post(
                "/api/v1/previews",
                headers={"Content-Type": "multipart/form-data; boundary=example"},
                content=unread(),
            )
            assert blocked.status_code == 429, blocked.text
            assert 1 <= int(blocked.headers["Retry-After"]) <= 60
            assert blocked.json()["error"]["code"] == "preview_rate_limited"
            assert console.post("/samples/clean").status_code == 429

            preview_id = preview.json()["id"]
            assert api.post(f"/api/v1/previews/{preview_id}/apply").status_code == 200
            assert api.get("/api/v1/catalog").json()["revision"] == 1
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM previews").fetchone() == (3,)


def test_unavailable_redis_only_blocks_new_previews(client: TestClient, database: str) -> None:
    preview = client.post("/api/v1/previews", files=UPLOAD)
    assert preview.status_code == 201
    with socket() as closed_listener:
        closed_listener.bind(("127.0.0.1", 0))
        host, port = closed_listener.getsockname()
        with Redis(
            host=host,
            port=port,
            socket_connect_timeout=0.25,
            socket_timeout=0.25,
            retry=Retry(NoBackoff(), 0),
        ) as redis:
            app = create_app(database, preview_quota=RedisPreviewQuota(redis))
            with TestClient(app) as affected:
                response = affected.post("/api/v1/previews", files=UPLOAD)
                assert response.status_code == 503
                assert response.json()["error"]["code"] == "preview_quota_unavailable"
                preview_id = preview.json()["id"]
                assert affected.post(f"/api/v1/previews/{preview_id}/apply").status_code == 200
                assert affected.get("/api/v1/catalog").json()["revision"] == 1
                rollback = affected.post("/api/v1/catalog/rollback", json={"expected_revision": 1})
                assert rollback.status_code == 200
