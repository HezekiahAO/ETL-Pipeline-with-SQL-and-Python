"""Unit tests for the XML extractor (no database needed)."""
import textwrap

import pytest
from defusedxml.common import EntitiesForbidden
from xml.etree.ElementTree import ParseError

from etl.extract import batched, iter_orders, parse_ts

from conftest import DATA


def write(tmp_path, body):
    p = tmp_path / "t.xml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return p


def test_parses_fields_positions_and_items(tmp_path):
    p = write(tmp_path, """\
        <orders>
          <meta><x>ignored</x></meta>
          <order id=" ord-7 " modified="2024-03-01T10:00:00Z">
            <customer><name>Cables &amp; Co</name><email>A@B.COM</email><country/></customer>
            <order_date>03/05/2024</order_date><status>  </status>
            <items><item sku="x-1" qty="2"><name>Thing</name><price>$1,000.50</price></item>
                   <item qty="1"><price>5</price></item></items>
          </order>
          <order><items/></order>
        </orders>""")
    a, b = list(iter_orders(p))
    assert (a.position, b.position) == (1, 2)
    assert a.order_id == " ord-7 "                    # copied as-is: cleaning is SQL's job
    assert a.customer_name == "Cables & Co"           # entities decoded
    assert a.customer_country is None                  # empty element -> None
    assert a.status is None                            # whitespace only -> None
    assert [(i.line_no, i.sku, i.qty, i.price) for i in a.items] == [(1, "x-1", "2", "$1,000.50"), (2, None, "1", "5")]
    assert b.order_id is None and b.items == []


def test_batched_groups_without_losing_records():
    assert [len(x) for x in batched(range(10), 4)] == [4, 4, 2]
    assert list(batched([], 3)) == []


def test_parse_ts():
    assert parse_ts("2024-03-01T10:00:00Z").hour == 10
    assert parse_ts("2024-03-01T10:00:00").tzinfo is not None      # naive => UTC
    assert parse_ts("garbage") is None and parse_ts(None) is None and parse_ts("  ") is None


def test_malformed_xml_raises_after_yielding_complete_records(tmp_path):
    text = (DATA / "raw_orders_full.xml").read_text()
    p = tmp_path / "cut.xml"
    p.write_text(text[: len(text) // 3])
    seen = 0
    with pytest.raises(ParseError):
        for _ in iter_orders(p):
            seen += 1
    assert seen > 100                                  # the good prefix was streamed before the failure


def test_entity_expansion_attack_is_refused(tmp_path):
    p = write(tmp_path, """\
        <?xml version="1.0"?><!DOCTYPE lol [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;">]>
        <orders><order id="1"><status>&b;</status></order></orders>""")
    with pytest.raises(EntitiesForbidden):
        list(iter_orders(p))


def test_real_dataset_record_counts():
    assert sum(1 for _ in iter_orders(DATA / "raw_orders_full.xml")) == 1027
    assert sum(1 for _ in iter_orders(DATA / "raw_orders_incremental.xml")) == 139
