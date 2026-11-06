# ImportLens

ImportLens is a FastAPI demo for reviewing supplier price lists. Upload a CSV, compare it with the active catalog, then apply the preview or roll back the last import.

## Quick start

```sh
docker compose up --build --wait
```

Open the [review console](http://127.0.0.1:8010/) or [Swagger UI](http://127.0.0.1:8010/docs). Compose starts the API, PostgreSQL, Redis, Kafka and the event relay, and seeds 20,000 synthetic products. Set `IMPORTLENS_PORT` to use a different host port.

1. Open **Feed with errors** to see why zero prices and duplicate SKUs block an import.
2. Open **Corrected feed** and review the changes. Applying it adds 10 products and updates 150. The 10 SKUs missing from the file stay in the catalog.
3. Roll back the import to restore the previous catalog under a new revision.

To try your own file, use UTF-8 CSV with these columns. Prices are in USD.

```csv
sku,name,price
SKU-000011,Steel bottle 000011,53.30
```

## How it works

- [Domain and application](src/importlens/application/) define import rules and use cases independently of FastAPI, Polars and storage.
- **PostgreSQL** saves the preview, catalog versions and event journal. Apply uses the exact saved preview and returns `409` if its base revision is stale.
- **Redis** shares a budget of 12 previews per 60-second window across API instances. Requests over the limit get `429` and a `Retry-After` header.
- **Kafka** receives `catalog.applied` and `catalog.rolled_back` on `importlens.catalog-events.v1` through a transactional outbox. Delivery is at least once. Consumers can deduplicate using `event_id`.

<details>
<summary>Run without Docker</summary>

Use Python 3.12, uv and a PostgreSQL 16 database.

```sh
uv sync --locked
export IMPORTLENS_DATABASE_URL=postgresql://localhost/importlens
uv run --locked importlens migrate
uv run --locked importlens seed-demo
uv run --locked uvicorn importlens.web.main:app --host 127.0.0.1 --port 8010
```

Set `IMPORTLENS_REDIS_URL` before starting the API to enable the quota. For event delivery, create the Kafka topic with one partition and run `uv run --locked importlens relay` with `IMPORTLENS_KAFKA_BOOTSTRAP_SERVERS` and the same database URL.

</details>

## Checks

```sh
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest
```

Set `IMPORTLENS_TEST_DATABASE_URL`, `IMPORTLENS_TEST_REDIS_URL` and `IMPORTLENS_TEST_KAFKA_BOOTSTRAP_SERVERS` to include the integration tests. CI also exercises the Docker Compose stack. Run the [20,000 and 200,000 row benchmark](scripts/benchmark.py) with `uv run --locked python scripts/benchmark.py`.
