import csv
import re
from collections import Counter
from collections.abc import Sequence
from io import TextIOWrapper
from typing import BinaryIO, Protocol

from importlens.domain.catalog import Feed, FeedItem, Issue, IssueCode, Item, Severity

MAX_ROWS = 200_000
_COLUMNS = frozenset({"sku", "name", "price"})
_SKU = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_PRICE = re.compile(r"([0-9]{1,7})(?:\.([0-9]{1,2}))?")


def _error(code: IssueCode, message: str, line: int, sku: str | None = None) -> Issue:
    return Issue(Severity.ERROR, code, message, line, sku)


def _incomplete(rows: int, issues: list[Issue], issue: Issue) -> Feed:
    return Feed(rows, (), frozenset(), (*issues, issue), complete=False)


def _price_cents(value: str) -> int:
    match = _PRICE.fullmatch(value)
    if match is None:
        return 0
    return int(match[1]) * 100 + int((match[2] or "").ljust(2, "0"))


class CsvRows(Protocol):
    @property
    def line_num(self) -> int: ...

    def __next__(self) -> list[str]: ...


class CsvFeedReader:
    def read(self, source: BinaryIO) -> Feed:
        stream = TextIOWrapper(source, encoding="utf-8-sig", newline="")
        try:
            return self._read_rows(csv.reader(stream, strict=True))
        finally:
            stream.detach()

    @staticmethod
    def _read_rows(reader: CsvRows) -> Feed:
        rows = 0
        line = 1
        issues: list[Issue] = []
        seen: Counter[str] = Counter()
        locations: list[tuple[str, int]] = []
        items: list[FeedItem] = []
        try:
            header = next(reader, [])
            if len(header) != len(_COLUMNS) or set(header) != _COLUMNS:
                return _incomplete(
                    rows, issues, _error(IssueCode.HEADER, "Expected columns sku, name, price.", 1)
                )

            indexes = tuple(header.index(column) for column in ("sku", "name", "price"))
            while True:
                line = reader.line_num + 1
                row = next(reader, None)
                if row is None:
                    break
                if not row:
                    continue
                rows += 1
                if rows > MAX_ROWS:
                    return _incomplete(
                        rows, issues, _error(IssueCode.ROW_LIMIT, "At most 200,000 rows.", line)
                    )
                item = _parse_row(row, indexes, line, seen, locations, issues)
                if item is not None:
                    items.append(item)
        except UnicodeDecodeError:
            return _incomplete(
                rows, issues, Issue(Severity.ERROR, IssueCode.ENCODING, "Use UTF-8.")
            )
        except csv.Error:
            return _incomplete(
                rows, issues, _error(IssueCode.MALFORMED, "CSV structure is invalid.", line)
            )

        if not rows:
            return _incomplete(
                rows, issues, _error(IssueCode.EMPTY, "Add at least one product row.", line)
            )
        for sku, row_line in locations:
            if seen[sku] > 1:
                issues.append(
                    _error(IssueCode.DUPLICATE, "SKU occurs more than once.", row_line, sku)
                )
        unique = tuple(entry for entry in items if seen[entry.item.sku] == 1)
        return Feed(rows, unique, frozenset(seen), tuple(issues))


def _parse_row(
    row: Sequence[str],
    indexes: tuple[int, ...],
    line: int,
    seen: Counter[str],
    locations: list[tuple[str, int]],
    issues: list[Issue],
) -> FeedItem | None:
    sku_index, name_index, price_index = indexes
    sku = row[sku_index].strip() if sku_index < len(row) else ""
    valid_sku = _SKU.fullmatch(sku) is not None
    if valid_sku:
        sku = sku.upper()
        seen[sku] += 1
        locations.append((sku, line))
    if len(row) != len(indexes):
        issues.append(
            _error(
                IssueCode.COLUMNS,
                "Expected exactly three fields.",
                line,
                sku if valid_sku else None,
            )
        )
        return None
    if not valid_sku:
        issues.append(
            _error(IssueCode.SKU, "Use 1–64 letters, digits, dots, hyphens or underscores.", line)
        )
        return None
    name = row[name_index].strip()
    cents = _price_cents(row[price_index].strip())
    if not cents:
        issues.append(
            _error(IssueCode.PRICE, "Use a positive USD price with up to two decimals.", line, sku)
        )
    if not name or len(name) > 200 or "\x00" in name:
        issues.append(_error(IssueCode.NAME, "Use a product name of 1–200 characters.", line, sku))
        return None
    return FeedItem(Item(sku, name, cents), line) if cents else None
