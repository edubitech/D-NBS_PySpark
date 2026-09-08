"""
CDC harness for Project 1 — FinTech Transaction Ledger & CDC Engine.

Real PaySim data (account_id, tx_type, amount) now drives every transaction —
read sequentially from PS_20174392719_1491204439457_log.csv (nameOrig / type /
amount columns). Everything downstream of that (envelope shape, the CDC
lifecycle, injected behaviours, output layout) is exactly the synthetic
design this file always had: PaySim is a flat transaction log, not a CDC
stream, so the create -> settle/chargeback lifecycle, late arrivals,
duplicate keys, out-of-order LSNs and schema drift are still invented here —
only the underlying transaction values are real.

Anchors step 1 = 2026-01-01 00:00:00 UTC and emits one JSON file per hour to

    ./_harness_output/raw/cdc/dt=YYYY-MM-DD/cdc_events_HH.json

for --days consecutive days (default 20), newline-delimited Debezium-style
envelopes. Run generate_and_upload() to also sync the output to S3.

Injected behaviours (rates are per emitted event unless noted):
  - late arrivals            3%    source.ts_ms backdated 1-5 days
  - duplicate PK in one file 1% of files  same transaction_id, different ts_ms
  - delete (chargeback)      0.5%  op="d"
  - out-of-order LSN         2%    correct lsn value, scrambled file position
  - schema drift             once, from day 12 onward: "risk_score" field
"""
from __future__ import annotations

import argparse
import csv
import itertools
import json
import random
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

ANCHOR = datetime(2026, 1, 1, tzinfo=timezone.utc)
CURRENCIES = ["USD", "EUR", "GBP"]

LATE_ARRIVAL_RATE = 0.03
DUPLICATE_FILE_RATE = 0.01
DELETE_RATE = 0.005
OUT_OF_ORDER_RATE = 0.02
SCHEMA_DRIFT_DAY = 12

random.seed(42)

# Real transactions: account_id, tx_type and amount come from the real PaySim
# CSV (nameOrig / type / amount) instead of being invented. Read lazily and
# cycled — with tens of thousands of events needed across 20 days, this never
# has to materialise the full 6.36M-row file at once.
REAL_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "PS_20174392719_1491204439457_log.csv"


def _real_rows():
    with REAL_DATA_PATH.open() as f:
        for row in csv.DictReader(f):
            yield row["nameOrig"], row["type"], round(float(row["amount"]), 2)


_REAL_ROWS = itertools.cycle(_real_rows())

# currency is assigned once per real account_id the first time it's seen,
# then cached so the same account is always billed in the same currency
ACCOUNT_CURRENCY: dict[str, str] = {}


def currency_for(account_id: str) -> str:
    if account_id not in ACCOUNT_CURRENCY:
        ACCOUNT_CURRENCY[account_id] = random.choice(CURRENCIES)
    return ACCOUNT_CURRENCY[account_id]


class LsnCounter:
    def __init__(self):
        self.value = 0

    def next(self) -> int:
        self.value += 1
        return self.value


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def make_transaction_id(day_idx: int, hour: int, seq: int) -> str:
    return f"T-{day_idx:03d}{hour:02d}{seq:04d}"


def build_envelope(op: str, transaction_id: str, account_id: str, amount: float,
                    tx_type: str, status: str, before: dict | None,
                    source_ts: datetime, arrival_ts: datetime, lsn: int,
                    add_risk_score: bool) -> dict:
    after = None
    if op != "d":
        after = {
            "transaction_id": transaction_id,
            "account_id": account_id,
            "amount": round(amount, 2),
            "currency": currency_for(account_id),
            "tx_type": tx_type,
            "status": status,
        }
        if add_risk_score:
            after["risk_score"] = round(random.uniform(0, 1), 3)

    return {
        "op": op,
        "ts_ms": ms(arrival_ts),
        "source": {
            "table": "transactions",
            "lsn": lsn,
            "ts_ms": ms(source_ts),
        },
        "before": before,
        "after": after,
    }


def generate_hour(day_idx: int, hour: int, lsn_counter: LsnCounter,
                   open_transactions: dict) -> list[dict]:
    """Returns the list of envelopes for one hour, mutating open_transactions
    (transaction_id -> last known {account_id, amount, tx_type, status}) in place."""
    hour_dt = ANCHOR + timedelta(days=day_idx, hours=hour)
    events: list[dict] = []
    add_risk_score = day_idx >= SCHEMA_DRIFT_DAY

    # 1) new transactions created this hour — account_id, tx_type and amount
    # are real PaySim values; only the CDC lifecycle around them is invented.
    for seq in range(random.randint(30, 80)):
        transaction_id = make_transaction_id(day_idx, hour, seq)
        account_id, tx_type, amount = next(_REAL_ROWS)
        status = "PENDING"

        source_ts = hour_dt
        if random.random() < LATE_ARRIVAL_RATE:
            source_ts = hour_dt - timedelta(days=random.randint(1, 5))

        env = build_envelope(
            op="c", transaction_id=transaction_id, account_id=account_id,
            amount=amount, tx_type=tx_type, status=status, before=None,
            source_ts=source_ts, arrival_ts=hour_dt,
            lsn=lsn_counter.next(), add_risk_score=add_risk_score,
        )
        events.append(env)
        open_transactions[transaction_id] = {
            "account_id": account_id, "amount": amount, "tx_type": tx_type,
            "status": status,
        }

    # 2) settle / chargeback a slice of currently-open transactions
    pending_ids = [tid for tid, t in open_transactions.items() if t["status"] == "PENDING"]
    random.shuffle(pending_ids)
    to_process = pending_ids[: max(1, len(pending_ids) // 2)]

    for transaction_id in to_process:
        tx = open_transactions[transaction_id]
        before = {"transaction_id": transaction_id, "status": tx["status"],
                  "amount": tx["amount"]}

        source_ts = hour_dt
        if random.random() < LATE_ARRIVAL_RATE:
            source_ts = hour_dt - timedelta(days=random.randint(1, 5))

        if random.random() < DELETE_RATE:
            env = build_envelope(
                op="d", transaction_id=transaction_id, account_id=tx["account_id"],
                amount=tx["amount"], tx_type=tx["tx_type"], status=tx["status"],
                before=before, source_ts=source_ts, arrival_ts=hour_dt,
                lsn=lsn_counter.next(), add_risk_score=False,
            )
            events.append(env)
            tx["status"] = "REVERSED"
        else:
            env = build_envelope(
                op="u", transaction_id=transaction_id, account_id=tx["account_id"],
                amount=tx["amount"], tx_type=tx["tx_type"], status="SETTLED",
                before=before, source_ts=source_ts, arrival_ts=hour_dt,
                lsn=lsn_counter.next(), add_risk_score=add_risk_score,
            )
            events.append(env)
            tx["status"] = "SETTLED"

    # 3) duplicate primary key within this file — same event, different ts_ms
    if events and random.random() < DUPLICATE_FILE_RATE:
        original = random.choice(events)
        dup = json.loads(json.dumps(original))  # deep copy
        dup["ts_ms"] = ms(hour_dt + timedelta(minutes=random.randint(1, 30)))
        events.append(dup)

    # 4) out-of-order LSN — correct lsn value, scrambled write position
    n_swaps = max(1, int(len(events) * OUT_OF_ORDER_RATE))
    for _ in range(n_swaps):
        if len(events) < 2:
            break
        i = random.randint(0, len(events) - 2)
        events[i], events[i + 1] = events[i + 1], events[i]

    return events


def generate(days: int, out_dir: Path) -> None:
    lsn_counter = LsnCounter()
    open_transactions: dict = {}
    total_events = 0

    for day_idx in range(days):
        day_str = (ANCHOR + timedelta(days=day_idx)).strftime("%Y-%m-%d")
        day_dir = out_dir / "raw" / "cdc" / f"dt={day_str}"
        day_dir.mkdir(parents=True, exist_ok=True)

        for hour in range(24):
            events = generate_hour(day_idx, hour, lsn_counter, open_transactions)
            file_path = day_dir / f"cdc_events_{hour:02d}.json"
            with file_path.open("w") as f:
                for env in events:
                    f.write(json.dumps(env) + "\n")
            total_events += len(events)

        print(f"day {day_idx:02d} ({day_str}): "
              f"{len(open_transactions)} tracked transactions so far")

    print(f"\ndone. {total_events} total events across {days} days "
          f"-> {out_dir / 'raw' / 'cdc'}")


def upload(out_dir: Path, s3_prefix: str) -> None:
    src = str(out_dir / "raw" / "cdc") + "/"
    dst = s3_prefix.rstrip("/") + "/"
    print(f"syncing {src} -> {dst}")
    subprocess.run(["aws", "s3", "sync", src, dst], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--out-dir", type=Path, default=Path("./_harness_output"))
    parser.add_argument("--upload-to", type=str, default=None,
                         help="e.g. s3://amp-ali-dlh-raw/fintech_cdc/raw/cdc")
    args = parser.parse_args()

    generate(args.days, args.out_dir)

    if args.upload_to:
        upload(args.out_dir, args.upload_to)
