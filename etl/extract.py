"""
EXTRACT: stream an XML export and yield flat, still-uncleaned records in controlled batches.

* Uses iterparse (streaming) so memory stays flat however large the file is.
* defusedxml refuses entity-expansion / external-entity attacks ("billion laughs", XXE).
* No cleaning happens here. Values are copied as text exactly as found; only
  blank/missing elements become None. All real cleaning is done in SQL.

Expected structure (see data/raw_orders_full.xml):

  <orders>
    <order id=".." modified="..">
      <customer><name/><email/><country/></customer>
      <order_date/><status/><channel/>
      <items><item sku=".." qty=".."><name/><price/></item> ...</items>
    </order>
    ...
  </orders>
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from defusedxml.ElementTree import iterparse


@dataclass
class RawItem:
    line_no: int
    sku: str | None
    name: str | None
    qty: str | None
    price: str | None


@dataclass
class RawOrder:
    position: int                      # 1-based position of the <order> element in the file
    order_id: str | None
    modified: str | None
    customer_name: str | None
    customer_email: str | None
    customer_country: str | None
    order_date: str | None
    status: str | None
    channel: str | None
    items: list[RawItem] = field(default_factory=list)

    def modified_ts(self) -> datetime | None:
        """Best-effort parse used ONLY to decide skipping/watermarks; SQL re-parses authoritatively."""
        return parse_ts(self.modified)


def parse_ts(value: str | None) -> datetime | None:
    if not value or not value.strip():
        return None
    try:
        ts = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _text(parent, path: str) -> str | None:
    el = parent.find(path)
    if el is None or el.text is None or not el.text.strip():
        return None
    return el.text


def _attr(el, name: str) -> str | None:
    v = el.get(name)
    return v if v is not None and v.strip() else None


def _to_order(position: int, el) -> RawOrder:
    items = []
    for n, it in enumerate(el.findall("items/item"), start=1):
        items.append(RawItem(n, _attr(it, "sku"), _text(it, "name"), _attr(it, "qty"), _text(it, "price")))
    return RawOrder(
        position=position, order_id=_attr(el, "id"), modified=_attr(el, "modified"),
        customer_name=_text(el, "customer/name"), customer_email=_text(el, "customer/email"),
        customer_country=_text(el, "customer/country"), order_date=_text(el, "order_date"),
        status=_text(el, "status"), channel=_text(el, "channel"), items=items)


def iter_orders(path: Path) -> Iterator[RawOrder]:
    """Yield one RawOrder per <order>, in document order. Malformed XML raises xml.etree.ElementTree.ParseError."""
    position = 0
    for _event, el in iterparse(str(path), events=("end",)):
        if el.tag != "order":
            continue
        position += 1
        yield _to_order(position, el)
        el.clear()                                   # free the subtree immediately


def batched(records: Iterable, size: int) -> Iterator[list]:
    """Group any iterable into lists of at most `size` items."""
    batch: list = []
    for r in records:
        batch.append(r)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
