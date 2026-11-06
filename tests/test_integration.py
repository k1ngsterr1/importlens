import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import httpx2
import psycopg
import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from fastapi.testclient import TestClient

from importlens.adapters.kafka import KafkaPublisher
from importlens.adapters.postgres.outbox import PostgresOutbox
from importlens.application.event_relay import EventRelay
from importlens.application.imports import MAX_UPLOAD_BYTES
from importlens.bootstrap import build_imports, connection_pool
from importlens.domain.catalog import Item

API = "/api/v1"


def create_preview(client: TestClient, rows: str) -> str:
    response = client.post(
        f"{API}/previews",
        files={"file": ("vendor.csv", f"sku,name,price\n{rows}".encode(), "text/csv")},
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


def apply(client: TestClient, preview_id: str) -> httpx2.Response:
    return client.post(f"{API}/previews/{preview_id}/apply")


def test_apply_exact_preview_preserve_missing_and_rollback(
    client: TestClient, database: str
) -> None:
    original = create_preview(client, "A,Alpha,10\nB,Beta,12\n")
    initial = apply(client, original).json()
    preview_id = create_preview(client, "A,Alpha renamed,15\nC,Gamma,9.12\n")
    changes = client.get(f"{API}/previews/{preview_id}/changes").json()
    assert [change["sku"] for change in changes] == ["A", "C", "B"]
    assert changes[0]["before"]["price_cents"] == 1000
    assert changes[0]["after"]["price_cents"] == 1500
    filtered = client.get(f"{API}/previews/{preview_id}/changes", params={"kind": "missing"})
    assert [change["sku"] for change in filtered.json()] == ["B"]
    applied = apply(client, preview_id)
    assert applied.status_code == 200, applied.text
    event = applied.json()
    assert apply(client, preview_id).json() == event
    state = client.get(f"{API}/catalog").json()
    assert (state["revision"], state["item_count"]) == (2, 3)

    with connection_pool(database) as pool:
        catalog = build_imports(pool).catalog
        assert set(catalog.snapshot(state["version_id"])) == {
            Item("A", "Alpha renamed", 1500),
            Item("B", "Beta", 1200),
            Item("C", "Gamma", 912),
        }
        assert set(catalog.snapshot(initial["version_id"])) == {
            Item("A", "Alpha", 1000),
            Item("B", "Beta", 1200),
        }

    rollback = client.post(f"{API}/catalog/rollback", json={"expected_revision": 2})
    assert rollback.status_code == 200
    assert rollback.json()["version_id"] == initial["version_id"]
    assert (
        client.post(f"{API}/catalog/rollback", json={"expected_revision": 2}).json()
        == rollback.json()
    )
    assert apply(client, preview_id).json() == event
    restored = client.get(f"{API}/catalog").json()
    assert (restored["revision"], restored["item_count"], restored["can_rollback"]) == (3, 2, False)


def test_parallel_apply_and_aba(client: TestClient) -> None:
    first = create_preview(client, "A,Alpha,10\n")
    second = create_preview(client, "B,Beta,12\n")
    assert client.get(f"{API}/catalog").json()["revision"] == 0
    barrier = Barrier(2)

    def submit(preview_id: str) -> httpx2.Response:
        barrier.wait(timeout=5)
        return apply(client, preview_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(submit, [first, second]))
    assert sorted(response.status_code for response in responses) == [200, 409]
    stale = first if responses[0].status_code == 409 else second
    assert client.post(f"{API}/catalog/rollback", json={"expected_revision": 1}).status_code == 200
    assert client.get(f"{API}/catalog").json()["item_count"] == 0
    conflict = apply(client, stale)
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "stale_revision"


def test_blocked_no_changes_and_request_limit(client: TestClient) -> None:
    assert apply(client, create_preview(client, "A,Alpha,10\n")).status_code == 200
    blocked = create_preview(client, "A,Invalid,0\nB,Beta,12\nb,Beta duplicate,13\n")
    assert apply(client, blocked).json()["error"]["code"] == "preview_blocked"
    issues = client.get(f"{API}/previews/{blocked}/issues").json()
    assert [issue["code"] for issue in issues] == ["price", "duplicate", "duplicate"]
    unchanged = create_preview(client, "A,Alpha,10\n")
    assert apply(client, unchanged).json()["error"]["code"] == "nothing_to_apply"
    assert client.get(f"{API}/previews/{uuid4()}/issues").status_code == 404
    assert (
        client.post(
            f"{API}/previews", headers={"Content-Length": str(MAX_UPLOAD_BYTES * 2)}
        ).status_code
        == 413
    )
    assert client.get(f"{API}/catalog").json()["revision"] == 1


def test_apply_failure_rolls_back_entire_write(client: TestClient, database: str) -> None:
    preview_id = create_preview(client, "A,Alpha,10\n")
    with psycopg.connect(database, autocommit=True) as connection:
        connection.execute(
            "ALTER TABLE catalog_events ADD CONSTRAINT reject_apply CHECK (revision > 2) NOT VALID"
        )
        with pytest.raises(psycopg.errors.CheckViolation):
            apply(client, preview_id)
        assert client.get(f"{API}/catalog").json()["revision"] == 0
        assert connection.execute("SELECT count(*) FROM catalog_versions").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM catalog_items").fetchone() == (0,)
        connection.execute("ALTER TABLE catalog_events DROP CONSTRAINT reject_apply")
    assert apply(client, preview_id).status_code == 200


def test_console_routes(client: TestClient) -> None:
    workspace = client.get("/")
    assert workspace.status_code == 200
    assert "Supplier imports" in workspace.text
    result = client.post(
        "/upload",
        files={"file": ("<script>.csv", b"sku,name,price\nA,Alpha,1\n", "text/csv")},
    )
    assert result.status_code == 200
    assert "Ready to apply" in result.text
    assert "&lt;script&gt;.csv" in result.text
    assert client.get("/samples/clean.csv").headers["content-type"].startswith("text/csv")


def test_parallel_retry_of_same_preview(client: TestClient) -> None:
    preview_id = create_preview(client, "A,Alpha,10\n")
    barrier = Barrier(2)

    def submit(_: int) -> httpx2.Response:
        barrier.wait(timeout=5)
        return apply(client, preview_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.map(submit, range(2))
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert client.get(f"{API}/catalog").json()["revision"] == 1


def test_streamed_upload_limit(client: TestClient, database: str) -> None:
    boundary = "importlens-test"

    def chunks() -> Iterator[bytes]:
        yield (
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="large.csv"\r\n'
            "Content-Type: text/csv\r\n\r\n"
        ).encode()
        chunk = b"x" * (1024 * 1024)
        for _ in range(25):
            yield chunk
        yield f"\r\n--{boundary}--\r\n".encode()

    response = client.post(
        f"{API}/previews",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        content=chunks(),
    )
    assert response.status_code == 413, response.text
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM previews").fetchone() == (0,)


def test_kafka_events_roundtrip(client: TestClient, database: str) -> None:
    servers = os.environ.get("IMPORTLENS_TEST_KAFKA_BOOTSTRAP_SERVERS")
    if servers is None:
        pytest.skip("Set IMPORTLENS_TEST_KAFKA_BOOTSTRAP_SERVERS for the Kafka round-trip.")
    topic = f"importlens-test-{uuid4()}"
    admin = AdminClient({"bootstrap.servers": servers})
    admin.create_topics([NewTopic(topic, 1, 1)], request_timeout=15)[topic].result(timeout=16)

    try:
        preview = create_preview(client, "A,Alpha,10\n")
        first = apply(client, preview)
        assert first.status_code == 200
        assert apply(client, preview).json() == first.json()
        rollback = client.post(f"{API}/catalog/rollback", json={"expected_revision": 1})
        assert rollback.status_code == 200
        assert client.post(f"{API}/catalog/rollback", json={"expected_revision": 1}).json() == (
            rollback.json()
        )

        with (
            connection_pool(database) as pool,
            psycopg.connect(database, autocommit=True) as connection,
        ):
            outbox = PostgresOutbox(pool)
            connection.execute("""
                ALTER TABLE catalog_event_delivery ADD CONSTRAINT prevent_checkpoint
                    CHECK (last_published_revision = 0) NOT VALID
                """)
            with pytest.raises(psycopg.errors.CheckViolation):
                EventRelay(outbox, KafkaPublisher(servers, topic)).deliver_next()
            connection.execute(
                "ALTER TABLE catalog_event_delivery DROP CONSTRAINT prevent_checkpoint"
            )
            restarted = EventRelay(outbox, KafkaPublisher(servers, topic))
            assert restarted.deliver_next()
            assert restarted.deliver_next()
            assert not restarted.deliver_next()

        with Consumer({"bootstrap.servers": servers, "group.id": topic}) as consumer:
            consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
            records = []
            for _ in range(3):
                record = consumer.poll(10)
                assert record is not None and record.error() is None
                payload = record.value()
                assert payload is not None
                records.append((record.key(), json.loads(payload)))
            assert consumer.poll(0.5) is None

        original, duplicate, undone = [value for _, value in records]
        assert duplicate == original
        assert (original["type"], undone["type"]) == ("catalog.applied", "catalog.rolled_back")
        assert (original["revision"], undone["revision"]) == (1, 2)
        assert (original["preview_id"], undone["preview_id"]) == (preview, None)
        assert original["version_id"] == undone["previous_version_id"]
        assert original["previous_version_id"] == undone["version_id"]
        for key, value in records:
            assert key == value["catalog_id"].encode()
            assert value["event_id"] == f"{value['catalog_id']}:{value['revision']}"
            assert value["schema_version"] == 1
    finally:
        admin.delete_topics([topic], request_timeout=10)[topic].result(timeout=11)


def test_cross_origin_form_cannot_rollback(client: TestClient) -> None:
    assert apply(client, create_preview(client, "A,Alpha,10\n")).status_code == 200
    data = {"revision": "1"}
    cross_origin = client.post("/rollback", data=data, headers={"Origin": "https://other.example"})
    assert cross_origin.status_code == 403
    assert client.get(f"{API}/catalog").json()["revision"] == 1
    same_origin = client.post("/rollback", data=data, headers={"Origin": "http://testserver"})
    assert same_origin.status_code == 200
    assert client.get(f"{API}/catalog").json()["revision"] == 2
