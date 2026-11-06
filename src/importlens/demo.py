import csv
from enum import StrEnum
from io import BytesIO, StringIO

DEMO_ROWS = 20_000
_PRODUCTS = ("Canvas tote", "Steel bottle", "Travel mug", "Desk mat", "Cable organizer")


class DemoFeed(StrEnum):
    BASELINE = "baseline"
    CLEAN = "clean"
    WITH_ERRORS = "with-errors"


def generate_feed(size: int, variant: DemoFeed) -> bytes:
    target = StringIO(newline="")
    writer = csv.writer(target)
    writer.writerow(("sku", "name", "price"))
    for index in range(1, size + 1):
        sku_index = index
        cents = 1290 + index % 20 * 125
        if variant != DemoFeed.BASELINE:
            if index <= 10:
                sku_index = size + index
            elif index <= 24:
                cents *= 2
            elif index <= 160:
                cents += 100
            if variant == DemoFeed.WITH_ERRORS:
                if index in (31, 32, 33):
                    cents = 0
                if index == 40:
                    sku_index = 39
        name = _PRODUCTS[sku_index % len(_PRODUCTS)]
        writer.writerow(
            (f"SKU-{sku_index:06d}", f"{name} {sku_index:06d}", f"{cents // 100}.{cents % 100:02d}")
        )
    return target.getvalue().encode()


def demo_upload(variant: DemoFeed) -> tuple[str, BytesIO]:
    return f"supplier-{variant}.csv", BytesIO(generate_feed(DEMO_ROWS, variant))
