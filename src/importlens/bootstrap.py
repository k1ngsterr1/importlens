import os
from collections.abc import Iterator
from contextlib import contextmanager

from psycopg_pool import ConnectionPool
from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from importlens.adapters.csv_feed import CsvFeedReader
from importlens.adapters.polars_changes import PolarsChangeDetector
from importlens.adapters.postgres.catalog import PgConnection, PostgresCatalog
from importlens.adapters.redis_quota import RedisPreviewQuota
from importlens.application.imports import Imports
from importlens.application.preview_quota import PreviewQuota


def database_url() -> str:
    return os.environ["IMPORTLENS_DATABASE_URL"]


def connection_pool(url: str) -> ConnectionPool[PgConnection]:
    return ConnectionPool(url, min_size=1, max_size=4, timeout=5, open=False)


def build_imports(pool: ConnectionPool[PgConnection]) -> Imports:
    return Imports(PostgresCatalog(pool), CsvFeedReader(), PolarsChangeDetector())


@contextmanager
def configured_preview_quota() -> Iterator[PreviewQuota | None]:
    url = os.environ.get("IMPORTLENS_REDIS_URL")
    if url is None:
        yield None
        return

    with Redis.from_url(
        url,
        socket_connect_timeout=0.25,
        socket_timeout=0.25,
        retry=Retry(NoBackoff(), 0),
        max_connections=16,
    ) as client:
        yield RedisPreviewQuota(client)
