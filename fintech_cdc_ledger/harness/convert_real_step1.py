"""
Bare-minimum proof: convert real PaySim step=1 rows (2,708 transactions,
real isFraud labels) into the exact same Debezium CDC envelope shape the
existing pipeline already reads. No new Spark code, no changes to
bronze_ingest.py — this just produces a JSON file at a fresh, clearly
labeled extract_date (2026-02-01) so it never collides with the synthetic
harness's dates (2026-01-01..06).

One 'c' (create) event per real transaction, status=SETTLED (PaySim rows
represent already-completed transactions, so no synthetic PENDING lifecycle
is invented here — that's an intentional simplification for this minimal
proof, not a claim that real CDC would look like this).

isFraud / isFlaggedFraud are carried through in `after` for future use
(e.g. Step 6b's Q2 precision/recall check) — harmless extra fields the
existing dedup/merge jobs simply ignore.
"""
import csv
import json
from datetime import datetime, timezone

SRC = "fintech_cdc_ledger/data/paysim_step1.csv"
OUT_DIR = "fintech_cdc_ledger/_real_proof_output/raw/cdc/dt=2026-02-01"
ANCHOR = datetime(2026, 1, 1, tzinfo=timezone.utc)  # step 1 = this instant, per brief

import os
os.makedirs(OUT_DIR, exist_ok=True)

source_ts_ms = int(ANCHOR.timestamp() * 1000)
arrival_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

count = 0
with open(SRC) as f, open(f"{OUT_DIR}/real_paysim_step1.json", "w") as out:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader, start=1):
        envelope = {
            "op": "c",
            "ts_ms": arrival_ts_ms,
            "source": {"table": "transactions", "lsn": i, "ts_ms": source_ts_ms},
            "before": {"note": "create_event_no_prior_state"},
            "after": {
                "transaction_id": f"PS-step1-{i:06d}",
                "account_id": row["nameOrig"],
                "amount": float(row["amount"]),
                "currency": "USD",
                "tx_type": row["type"],
                "status": "SETTLED",
                "is_fraud": int(row["isFraud"]),
                "is_flagged_fraud": int(row["isFlaggedFraud"]),
            },
        }
        out.write(json.dumps(envelope) + "\n")
        count += 1

print(f"wrote {count} real CDC envelopes to {OUT_DIR}/real_paysim_step1.json")
