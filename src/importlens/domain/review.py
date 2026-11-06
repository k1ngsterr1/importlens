from collections import Counter
from collections.abc import Sequence

from importlens.domain.catalog import (
    Change,
    ChangeKind,
    Comparison,
    Feed,
    Issue,
    IssueCode,
    Item,
    Severity,
    Summary,
    price_jump,
)

MAX_CATALOG_ITEMS = 250_000


def review(catalog: Sequence[Item], feed: Feed, different_skus: Sequence[str]) -> Comparison:
    current = {item.sku: item for item in catalog}
    incoming = {entry.item.sku: entry for entry in feed.items}
    issues = list(feed.issues)
    changes: list[Change] = []

    for sku in different_skus:
        entry = incoming[sku]
        before = current.get(sku)
        flagged = before is not None and price_jump(before, entry.item)
        kind = ChangeKind.ADDED if before is None else ChangeKind.UPDATED
        changes.append(Change(kind, sku, before, entry.item, flagged))
        if flagged:
            issues.append(
                Issue(
                    Severity.WARNING,
                    IssueCode.PRICE_JUMP,
                    "Price changed by at least 30%.",
                    entry.line,
                    sku,
                )
            )

    changes.extend(
        Change(ChangeKind.MISSING, sku, current[sku], None) for sku in current.keys() - feed.present
    )
    counts = Counter(change.kind for change in changes)
    if len(current) + counts[ChangeKind.ADDED] > MAX_CATALOG_ITEMS:
        issues.append(
            Issue(
                Severity.ERROR,
                IssueCode.CATALOG_LIMIT,
                f"The catalog can contain up to {MAX_CATALOG_ITEMS:,} products.",
            )
        )
    severities = Counter(issue.severity for issue in issues)
    summary = Summary(
        feed.rows,
        counts[ChangeKind.ADDED],
        counts[ChangeKind.UPDATED],
        counts[ChangeKind.MISSING],
        len(feed.items) - len(different_skus),
        severities[Severity.ERROR],
        severities[Severity.WARNING],
    )
    changes.sort(
        key=lambda change: (not change.flagged, change.kind == ChangeKind.MISSING, change.sku)
    )
    issues.sort(key=lambda issue: (issue.severity, issue.line or 0, issue.code))
    return Comparison(summary, tuple(changes), tuple(issues))
