import logging
import signal
from threading import Event
from types import FrameType

from importlens.application.event_relay import EventRelay, PublicationFailed

logger = logging.getLogger(__name__)


def run_relay(relay: EventRelay) -> None:
    stop = Event()

    def shutdown(_: int, __: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    delay = 0.5
    while not stop.is_set():
        try:
            published = relay.deliver_next()
        except PublicationFailed as exc:
            logger.warning("Catalog event delivery failed, retrying in %.1fs: %s", delay, exc)
            stop.wait(delay)
            delay = min(delay * 2, 15.0)
        else:
            delay = 0.5
            if not published:
                stop.wait(delay)
