import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

from importlens.adapters.postgres.migrate import migrate
from importlens.web.main import create_app


@pytest.fixture(autouse=True)
def ignore_runtime_redis_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IMPORTLENS_REDIS_URL", raising=False)


@pytest.fixture
def empty_database() -> Iterator[str]:
    url = os.environ.get("IMPORTLENS_TEST_DATABASE_URL")
    if url is None:
        pytest.skip("Set IMPORTLENS_TEST_DATABASE_URL for PostgreSQL integration tests.")
    schema = f"test_{uuid4().hex}"
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            yield make_conninfo(url, options=f"-c search_path={schema}")
        finally:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def database(empty_database: str) -> str:
    migrate(empty_database)
    return empty_database


@pytest.fixture
def client(database: str) -> Iterator[TestClient]:
    with TestClient(create_app(database)) as test_client:
        yield test_client
