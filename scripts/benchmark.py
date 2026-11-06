import argparse
from collections.abc import Iterator
from contextlib import contextmanager
from io import BytesIO
from statistics import median
from time import perf_counter
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from importlens.adapters.postgres.migrate import migrate
from importlens.bootstrap import build_imports, connection_pool, database_url
from importlens.demo import DemoFeed, generate_feed


@contextmanager
def isolated_schema(url: str) -> Iterator[str]:
    schema = f"bench_{uuid4().hex}"
    identifier = sql.Identifier(schema)
    with psycopg.connect(url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(identifier))
        try:
            yield make_conninfo(url, options=f"-c search_path={schema}")
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(identifier))


def benchmark(url: str, size: int, runs: int) -> None:
    baseline = generate_feed(size, DemoFeed.BASELINE)
    update = generate_feed(size, DemoFeed.CLEAN)
    previews: list[float] = []
    applies: list[float] = []

    with isolated_schema(url) as isolated:
        migrate(isolated)
        with connection_pool(isolated) as pool:
            imports = build_imports(pool)
            original = imports.create_preview("baseline.csv", BytesIO(baseline))
            imports.apply(original.id)
            for _ in range(runs):
                start = perf_counter()
                preview = imports.create_preview("vendor.csv", BytesIO(update))
                preview_done = perf_counter()
                applied = imports.apply(preview.id)
                apply_done = perf_counter()
                assert (
                    preview.summary.added,
                    preview.summary.updated,
                    preview.summary.missing,
                ) == (
                    10,
                    150,
                    10,
                )
                assert imports.catalog.state().item_count == size + 10
                imports.rollback(applied.revision)
                assert imports.catalog.state().item_count == size
                previews.append((preview_done - start) * 1000)
                applies.append((apply_done - preview_done) * 1000)

    print(
        f"{size:>7,} rows | {len(update) / 1024**2:5.2f} MiB | "
        f"preview {median(previews):7.1f} ms | apply {median(applies):7.1f} ms"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    for size in (20_000, 200_000):
        benchmark(database_url(), size, args.runs)


if __name__ == "__main__":
    main()
