import argparse
import logging
import os

from importlens.adapters.kafka import KafkaPublisher
from importlens.adapters.postgres.migrate import migrate
from importlens.adapters.postgres.outbox import PostgresOutbox
from importlens.application.event_relay import EventRelay
from importlens.bootstrap import build_imports, connection_pool, database_url
from importlens.demo import DemoFeed, demo_upload
from importlens.domain.errors import StaleRevision
from importlens.relay import run_relay


def main() -> None:
    parser = argparse.ArgumentParser(prog="importlens")
    parser.add_argument("command", choices=("migrate", "seed-demo", "relay"))
    command = parser.parse_args().command
    url = database_url()
    if command == "migrate":
        migrate(url)
        return

    if command == "relay":
        logging.basicConfig(level=logging.INFO)
        publisher = KafkaPublisher(os.environ["IMPORTLENS_KAFKA_BOOTSTRAP_SERVERS"])
        with connection_pool(url) as pool:
            run_relay(EventRelay(PostgresOutbox(pool), publisher))
        return

    with connection_pool(url) as pool:
        imports = build_imports(pool)
        if imports.catalog.state().revision != 0:
            return
        filename, source = demo_upload(DemoFeed.BASELINE)
        preview = imports.create_preview(filename, source)
        if preview.base_revision == 0:
            try:
                imports.apply(preview.id)
            except StaleRevision:
                return
