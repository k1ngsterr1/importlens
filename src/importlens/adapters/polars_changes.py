from collections.abc import Sequence

import polars as pl

from importlens.domain.catalog import Item


class PolarsChangeDetector:
    def changed_skus(self, catalog: Sequence[Item], incoming: Sequence[Item]) -> list[str]:
        previous = _frame(catalog)
        proposed = _frame(incoming)
        return (
            proposed.join(previous, on="sku", how="left", suffix="_old")
            .filter(
                pl.col("price_old").is_null()
                | (pl.col("price") != pl.col("price_old"))
                | (pl.col("name") != pl.col("name_old"))
            )
            .get_column("sku")
            .to_list()
        )


def _frame(items: Sequence[Item]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sku": [item.sku for item in items],
            "name": [item.name for item in items],
            "price": [item.price_cents for item in items],
        },
        schema={"sku": pl.String, "name": pl.String, "price": pl.Int64},
    )
