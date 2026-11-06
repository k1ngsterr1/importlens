from pathlib import Path

import psycopg

MIGRATIONS = Path(__file__).with_name("migrations")


def migrate(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(418291684)")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """)
        applied = {name for (name,) in connection.execute("SELECT name FROM schema_migrations")}
        for path in sorted(MIGRATIONS.glob("*.sql")):
            if path.name in applied:
                continue
            connection.execute(path.read_text(encoding="utf-8"))
            connection.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))
