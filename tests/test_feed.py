from io import BytesIO

import pytest

from importlens.adapters.csv_feed import CsvFeedReader
from importlens.adapters.polars_changes import PolarsChangeDetector
from importlens.domain.catalog import ChangeKind, IssueCode, Item, Severity
from importlens.domain.review import review


@pytest.mark.parametrize(
    ("data", "code"),
    [
        (b"", IssueCode.HEADER),
        (b"sku,name,price,sku\n", IssueCode.HEADER),
        (b"sku,name,price\n", IssueCode.EMPTY),
        (b"sku,name,price\nX,\xff,2\n", IssueCode.ENCODING),
        (b'sku,name,price\nX,"open quote,2\n', IssueCode.MALFORMED),
    ],
)
def test_structurally_invalid_csv(data: bytes, code: IssueCode) -> None:
    feed = CsvFeedReader().read(BytesIO(data))
    assert not feed.complete
    assert [issue.code for issue in feed.issues] == [code]


def test_prices_duplicates_bom_and_physical_lines() -> None:
    source = BytesIO(
        b'\xef\xbb\xbfprice,name,sku\r\n12.3,"Multi\nline",0001\r\n'
        b"0,Invalid,X\r\n2.01,Duplicate,x\r\n1.234,Too precise,other\r\n"
        b"9999999.99,Maximum,Z\r\n"
    )
    feed = CsvFeedReader().read(source)
    assert not source.closed
    assert feed.rows == 5
    assert [(entry.item, entry.line) for entry in feed.items] == [
        (Item("0001", "Multi\nline", 1230), 2),
        (Item("Z", "Maximum", 999999999), 7),
    ]
    assert feed.present == {"0001", "X", "OTHER", "Z"}
    assert {(issue.code, issue.line) for issue in feed.issues} == {
        (IssueCode.PRICE, 4),
        (IssueCode.PRICE, 6),
        (IssueCode.DUPLICATE, 4),
        (IssueCode.DUPLICATE, 5),
    }


def test_comparison_preserves_missing_and_flags_exact_threshold() -> None:
    base = [
        Item("A", "Changed", 1000),
        Item("B", "Missing", 600),
        Item("C", "Invalid", 1200),
        Item("E", "Rename", 700),
        Item("F", "Same", 990),
    ]
    feed = CsvFeedReader().read(
        BytesIO(b"sku,name,price\nA,Changed,7\nC,Invalid,0\nD,New,9\nE,Renamed,7\nF,Same,9.90\n")
    )
    different = PolarsChangeDetector().changed_skus(base, [entry.item for entry in feed.items])
    result = review(base, feed, different)
    assert result.summary.rows == 5
    assert result.summary.unchanged == 1
    assert result.summary.errors == result.summary.warnings == 1
    assert [(change.sku, change.kind, change.flagged) for change in result.changes] == [
        ("A", ChangeKind.UPDATED, True),
        ("D", ChangeKind.ADDED, False),
        ("E", ChangeKind.UPDATED, False),
        ("B", ChangeKind.MISSING, False),
    ]
    assert result.issues[1].severity == Severity.WARNING
    assert result.issues[1].line == 2


def test_row_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("importlens.adapters.csv_feed.MAX_ROWS", 1)
    feed = CsvFeedReader().read(BytesIO(b"sku,name,price\nA,First,1\nB,Second,1\n"))
    assert not feed.complete
    assert [(issue.code, issue.line) for issue in feed.issues] == [(IssueCode.ROW_LIMIT, 3)]
