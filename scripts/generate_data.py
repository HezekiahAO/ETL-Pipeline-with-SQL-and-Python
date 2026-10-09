#!/usr/bin/env python3
"""
Generates the demo dataset: deliberately messy XML order exports plus an
independent "ground truth" (expected_*.json) used to validate the pipeline.

  data/raw_orders_full.xml         initial export  (~1,000 records)
  data/raw_orders_incremental.xml  next export     (new + changed + re-sent + junk)
  data/expected_full.json          what a correct pipeline must have loaded after run 1
  data/expected_incremental.json   ... after run 2

Deterministic (fixed seed): re-running produces byte-identical files.
Usage: python scripts/generate_data.py
"""
import json
import random
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from xml.sax.saxutils import escape

DATA = Path(__file__).resolve().parent.parent / "data"
rng = random.Random(42)

FIRST = ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Kate", "Liam", "Mia",
         "Noah", "Olivia", "Pete", "Quinn", "Rosa", "Sam", "Tara", "Uma", "Victor", "Wendy", "Xavier", "Yusuf", "Zoe"]
LAST = ["Johnson", "Smith", "Brown", "Miller", "Davis", "Garcia", "Wilson", "Moore", "Taylor", "Anderson",
        "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Lee", "Walker", "Young", "King"]

CATALOG = [("KB-100", "Mechanical Keyboard", "89.99"), ("MS-200", "Wireless Mouse", "24.50"),               #[sku, product name, price]
           ("MN-300", "27in Monitor", "299.50"), ("CB-010", "USB-C Cable", "7.99"), ("HB-400", "USB-C Hub", "39.00"),
           ("WC-500", "HD Webcam", "59.00"), ("HP-600", "Noise Cancelling Headphones", "189.00"),
           ("SP-610", "Bluetooth Speaker", "49.95"), ("LP-700", "Laptop Stand", "34.90"), ("DK-710", "Standing Desk", "449.00"),
           ("CH-720", "Ergonomic Chair", "329.00"), ("SD-800", "1TB SSD", "99.00"), ("HD-810", "2TB External Drive", "74.99"),
           ("RT-900", "WiFi Router", "129.00"), ("SW-910", "Network Switch", "59.50"), ("PR-920", "Laser Printer", "219.00"),
           ("TB-930", "10in Tablet", "259.00"), ("PH-940", "Phone Case", "12.99"), ("CG-950", "Phone Charger", "19.99"),
           ("MC-960", "USB Microphone", "79.00"), ("LM-970", "LED Desk Lamp", "27.50"), ("CA-980", "Cables & Adapters", "15.00"),
           ("PW-990", "Power Bank", "35.00"), ("SV-999", "Server Rack", "1250.99")]

COUNTRY_VARIANTS = {"US": ["US", "USA", "United States", "U.S.A", "usa"], "GB": ["GB", "UK", "United Kingdom", "Great Britain"],
                    "DE": ["DE", "Germany", "Deutschland"], "CA": ["CA", "Canada"], "FR": ["FR", "France"],
                    "IN": ["IN", "India"], "AU": ["AU", "Australia"]}
STATUS_VARIANTS = {"shipped": ["shipped", "SHIPPED", " Shipped "], "delivered": ["delivered", "Delivered", "complete", "Completed"],
                   "pending": ["pending", "PENDING"], "processing": ["processing", " Processing"],
                   "cancelled": ["cancelled", "Canceled", "CANCELLED"]}
CHANNEL_VARIANTS = {"web": ["web", "Web", "online"], "mobile": ["mobile", "App", "MOBILE"],
                    "store": ["store", "In-Store", "retail"], "phone": ["phone", "Phone"]}

NEXT_STATUS = {"pending": "processing", "processing": "shipped", "shipped": "delivered"}

CUSTOMERS = []
for k in range(190):
    f, l = rng.choice(FIRST), rng.choice(LAST)
    CUSTOMERS.append({"name": f"{f} {l}", "email": f"{f}.{l}{k}@example.com".lower(),
                      "country": rng.choices(list(COUNTRY_VARIANTS), weights=[40, 15, 12, 10, 8, 8, 7])[0]})


def money(x):
    return Decimal(x)


def make_order(order_no, day, modified, status=None):
    c = rng.randrange(len(CUSTOMERS))           # picks a customer.
    skus = rng.sample(CATALOG, rng.choice([1, 1, 2, 2, 3, 4]))
    return {"order_no": order_no, "cust": c, "order_date": day, "modified": modified,
            "status": status or rng.choices(["delivered", "shipped", "cancelled", "pending", "processing"],
                                            weights=[45, 25, 8, 12, 10])[0],
            "channel": rng.choices(list(CHANNEL_VARIANTS), weights=[45, 30, 15, 10])[0],
            # missing optional values are a property of the record, so re-sent copies stay identical
            "miss_name": rng.random() < .03, "miss_country": rng.random() < .04, "miss_channel": rng.random() < .03,
            "items": [{"sku": s, "name": n, "qty": rng.choice([1, 1, 1, 2, 2, 3, 5]), "price": money(p)} for s, n, p in skus]}


def total(o):
    return sum(i["qty"] * i["price"] for i in o["items"])


# ---------------- messy rendering (values only; structure stays XML-valid) ----------------
def r_id(o):
    n = o["order_no"]
    return rng.choice([f"ORD-{n}", f"ORD-{n}", f"ord-{n}", str(n), f" ORD-{n} "])


def r_name(n):
    return rng.choice([n, n, n.lower(), n.upper(), "  " + n.replace(" ", "   ") + " "])


def r_email(e):
    return rng.choice([e, e, e.upper(), f" {e} "])


def r_date(d):
    k = rng.random()
    if k < .45:
        return d.isoformat()
    if k < .60:
        return f"{d.isoformat()}T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00Z"
    if k < .80:
        return d.strftime("%m/%d/%Y")
    if k < .95:
        return d.strftime("%d %b %Y")
    return str(int(datetime(d.year, d.month, d.day, 12, tzinfo=timezone.utc).timestamp()))


def r_price(p):
    s = f"{p:f}"
    if "." in s and rng.random() < .3:
        s = s.rstrip("0").rstrip(".")
    return rng.choice([s, s, f"${s}", f" ${s} "])


def r_qty(q):
    return rng.choice([str(q), str(q), f"{q}.0", f" {q} "])


def tag(name, value):
    return f"<{name}>{escape(value)}</{name}>" if value is not None else f"<{name}/>"


def render(o, fault=None):
    """Return one <order> element as a string. `fault` injects exactly one validation problem."""
    cu = CUSTOMERS[o["cust"]]
    oid = None if fault == "missing_order_id" else r_id(o)
    email = r_email(cu["email"])
    if fault == "bad_email":
        email = rng.choice(["not-an-email", "john at example dot com", "@example.com", "bob@"])
    if fault == "missing_email":
        email = None
    date_s = r_date(o["order_date"])
    if fault == "bad_date":
        date_s = rng.choice(["2024-02-30", "N/A", "yesterday", "31/31/2024", "2024-13-01"])
    if fault == "missing_date":
        date_s = None
    name = r_name(cu["name"])
    country = rng.choice(COUNTRY_VARIANTS[cu["country"]])
    status = rng.choice(STATUS_VARIANTS[o["status"]])
    channel = rng.choice(CHANNEL_VARIANTS[o["channel"]])
    if o["miss_name"]: name = None            # missing optional values (kept, defaulted downstream)
    if o["miss_country"]: country = None
    if o["miss_channel"]: channel = None
    items = [dict(i) for i in o["items"]]
    if fault == "bad_item":
        j = rng.randrange(len(items))
        kind = rng.choice(["qty_neg", "qty_word", "qty_zero", "price_free", "price_neg", "sku_missing"])
        if kind == "qty_neg": items[j]["qty"] = "-2"
        if kind == "qty_word": items[j]["qty"] = "two"
        if kind == "qty_zero": items[j]["qty"] = "0"
        if kind == "price_free": items[j]["price"] = "free"
        if kind == "price_neg": items[j]["price"] = "-5.00"
        if kind == "sku_missing": items[j]["sku"] = ""
    lines = []
    for it in items:
        qty = it["qty"] if isinstance(it["qty"], str) else r_qty(it["qty"])
        price = it["price"] if isinstance(it["price"], str) else r_price(it["price"])
        sku = it["sku"] if fault != "bad_item" or it["sku"] == "" else rng.choice([it["sku"], it["sku"].lower()])
        lines.append(f'      <item sku="{escape(sku)}" qty="{escape(qty)}">{tag("name", it["name"])}{tag("price", price)}</item>')
    items_xml = "<items/>" if fault == "no_items" else "<items>\n" + "\n".join(lines) + "\n    </items>"
    id_attr = "" if oid is None else f' id="{escape(oid)}"'
    mod = o["modified"].strftime("%Y-%m-%dT%H:%M:%SZ")
    return (f'  <order{id_attr} modified="{mod}">\n'
            f'    <customer>{tag("name", name)}{tag("email", email)}{tag("country", country)}</customer>\n'
            f'    {tag("order_date", date_s)}{tag("status", status)}{tag("channel", channel)}\n'
            f'    {items_xml}\n  </order>')


def write_xml(path, records, exported_at):
    path.write_text('<?xml version="1.0" encoding="UTF-8"?>\n'
                    f'<orders source="legacy-shop" exported_at="{exported_at}">\n' + "\n".join(records) + "\n</orders>\n",
                    encoding="utf-8")


def summarize(state, n_records, rejected, extra=None):
    final = [o for o in state.values()]
    by_status = {}
    for o in final:
        by_status[o["status"]] = by_status.get(o["status"], 0) + 1
    out = {"records_in_file": n_records, "valid_unique_orders": len(final),
           "total_revenue": str(sum(total(o) for o in final)),
           "distinct_customers": len({o["cust"] for o in final}),
           "distinct_products": len({i["sku"] for o in final for i in o["items"]}),
           "orders_by_status": dict(sorted(by_status.items())),
           "rejected_records": sum(rejected.values()), "rejected_by_reason": dict(sorted(rejected.items()))}
    if extra:
        out.update(extra)
    return out


# ===================================== FULL EXPORT =====================================
N_BASE = 900
start = date(2024, 1, 1)
orders = {}
for n in range(1, N_BASE + 1):
    day = start + timedelta(days=int((n - 1) / N_BASE * 85))                      # 1 Jan .. ~25 Mar
    mod = datetime(day.year, day.month, day.day, rng.randint(6, 20), rng.randint(0, 59), rng.randint(0, 59))
    orders[100000 + n] = make_order(100000 + n, day, mod)
ids = sorted(orders)

seq = [(render(orders[i]), i) for i in ids]                                       # position == id order
final_state = {i: deepcopy(orders[i]) for i in ids}

def insert_later(rec, after_idx):
    seq.insert(rng.randint(after_idx + 1, len(seq)), rec)

# (1) exact duplicates: the same record re-sent (rendered differently, same content)
for i in rng.sample(ids, 45):
    insert_later((render(orders[i]), i), seq.index(next(s for s in seq if s[1] == i)))

# (2) updated versions: v1 early, newer v2 later (status moves on and/or a quantity changes)
upd_ids = rng.sample([i for i in ids if orders[i]["order_date"] < date(2024, 3, 20) and orders[i]["status"] in NEXT_STATUS], 30)
for i in upd_ids:
    v2 = deepcopy(orders[i])
    v2["status"] = NEXT_STATUS[v2["status"]]
    v2["modified"] = min(orders[i]["modified"] + timedelta(days=rng.randint(1, 4)), datetime(2024, 3, 31, 23, 0, 0))
    if rng.random() < .4:
        v2["items"][0]["qty"] += 1
    final_state[i] = v2
    insert_later((render(v2), i), seq.index(next(s for s in seq if s[1] == i)))

# (3) out-of-order: newest version appears FIRST, stale version later
stale_ids = rng.sample([i for i in ids if i not in upd_ids and orders[i]["order_date"] < date(2024, 3, 20)
                        and orders[i]["status"] in NEXT_STATUS], 12)
for i in stale_ids:
    v2 = deepcopy(orders[i])
    v2["status"] = NEXT_STATUS[v2["status"]]
    v2["modified"] = orders[i]["modified"] + timedelta(days=2)
    final_state[i] = v2
    idx = next(k for k, s in enumerate(seq) if s[1] == i)
    seq[idx] = (render(v2), i)                                 # newest takes the original slot
    insert_later((render(orders[i]), i), idx)                  # stale re-appears later

# (4) invalid records, each hitting exactly ONE rule, each with its own id range
FAULTS = [("missing_order_id", 6), ("bad_email", 7), ("missing_email", 4), ("bad_date", 6), ("missing_date", 3),
          ("no_items", 5), ("bad_item", 9)]
REASON = {"missing_order_id": "missing_order_id", "bad_email": "invalid_or_missing_email",
          "missing_email": "invalid_or_missing_email", "bad_date": "invalid_or_missing_date",
          "missing_date": "invalid_or_missing_date", "no_items": "no_items", "bad_item": "invalid_item"}
rejected = {}
bad_no = 900000
for fault, cnt in FAULTS:
    for _ in range(cnt):
        bad_no += 1
        day = start + timedelta(days=rng.randint(0, 80))
        o = make_order(bad_no, day, datetime(day.year, day.month, day.day, 9, 0, 0))
        seq.insert(rng.randint(0, len(seq)), (render(o, fault=fault), None))
        rejected[REASON[fault]] = rejected.get(REASON[fault], 0) + 1

write_xml(DATA / "raw_orders_full.xml", [s[0] for s in seq], "2024-04-01T00:00:00Z")
full_max_mod = max(o["modified"] for o in final_state.values())
full_max_mod = max([full_max_mod] + [orders[i]["modified"] for i in ids])
exp_full = summarize(final_state, len(seq), rejected, {
    "exact_duplicate_records": 45, "updated_versions": len(upd_ids), "out_of_order_stale_versions": len(stale_ids),
    "high_watermark_after_load": full_max_mod.strftime("%Y-%m-%dT%H:%M:%SZ")})
(DATA / "expected_full.json").write_text(json.dumps(exp_full, indent=2) + "\n")

# ================================= INCREMENTAL EXPORT =================================
inc, rejected2 = [], {}
state2 = {i: deepcopy(o) for i, o in final_state.items()}
def april(day):
    return datetime(2024, 4, day, rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))

# new orders
new_ids = []
for n in range(N_BASE + 1, N_BASE + 61):
    d = rng.randint(1, 3)
    o = make_order(100000 + n, date(2024, 4, d), april(d))
    state2[100000 + n] = o
    new_ids.append(100000 + n)
    inc.append(render(o))
# changed existing orders (status advances, some quantities change)
changed = rng.sample([i for i in ids if state2[i]["status"] in NEXT_STATUS], 25)
for k, i in enumerate(changed):
    o = deepcopy(state2[i])
    o["status"] = NEXT_STATUS[o["status"]]
    o["modified"] = april(rng.randint(1, 3))
    if k < 10:
        o["items"][0]["qty"] += 1
    state2[i] = o
    inc.append(render(o))
# identical content re-sent with a fresh modified stamp (system re-export) => must be de-duplicated
unchanged = rng.sample([i for i in ids if i not in changed], 15)
for i in unchanged:
    o = deepcopy(state2[i])
    o["modified"] = april(rng.randint(1, 3))
    inc.append(render(o))
# old records re-sent with their ORIGINAL modified stamp => watermark filter should skip them
old_resent = rng.sample([i for i in ids if state2[i]["modified"] < full_max_mod - timedelta(days=1) and i not in changed
                         and i not in unchanged], 30)
for i in old_resent:
    inc.append(render(final_state[i]))
# a repeated new order inside the same file
for i in rng.sample(new_ids, 3):
    inc.append(render(state2[i]))
# junk
for fault, cnt in [("bad_email", 2), ("bad_date", 2), ("bad_item", 1), ("missing_order_id", 1)]:
    for _ in range(cnt):
        bad_no += 1
        o = make_order(bad_no, date(2024, 4, 2), april(2))
        inc.insert(rng.randint(0, len(inc)), render(o, fault=fault))
        rejected2[REASON[fault]] = rejected2.get(REASON[fault], 0) + 1
rng.shuffle(inc)
write_xml(DATA / "raw_orders_incremental.xml", inc, "2024-04-04T00:00:00Z")
exp_inc = summarize(state2, len(inc), rejected2, {
    "new_orders": len(new_ids), "changed_orders": len(changed), "identical_resent_with_new_timestamp": len(unchanged),
    "old_records_resent_skippable_by_watermark": len(old_resent), "repeated_new_orders_in_file": 3})
(DATA / "expected_incremental.json").write_text(json.dumps(exp_inc, indent=2) + "\n")

print(f"full file: {len(seq)} records -> {exp_full['valid_unique_orders']} valid unique orders, revenue {exp_full['total_revenue']}")
print(f"incremental file: {len(inc)} records -> {exp_inc['valid_unique_orders']} valid unique orders, revenue {exp_inc['total_revenue']}")
