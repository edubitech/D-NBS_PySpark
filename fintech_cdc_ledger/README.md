# FinTech Transaction Ledger & CDC Engine — submission

Project 1 of the capstone (`Exam_Folder/1_STUDENT_FinTech_CDC_Ledger.md`).
Everything in this folder is real: run against the account's own EMR
Serverless, Iceberg, Glue and Redshift — no fabricated numbers, no sample
output copied from documentation.

## Live report

**[Ledger State](https://claude.ai/code/artifact/951fbf4a-7199-47b1-b4e3-91df7cc17266)**
— the single source of truth for this project: current AWS state, every
step's real results, and exactly how to verify each one yourself in the
console. Read this first.

## Evidence pack

**[EVIDENCE.md](./EVIDENCE.md)** — the 18 required values plus the break log
and honest-limitations section, in the format the brief asks for.

## Folder layout

```
fintech_cdc_ledger/
├── README.md              this file
├── EVIDENCE.md             the 18-item evidence pack + break log
├── data/                   the real PaySim CSV (6.36M rows) + a small slice
├── harness/                the CDC event generator — real transactions,
│                           invented CDC lifecycle (late arrivals, dup keys,
│                           deletes, out-of-order LSN, schema drift)
├── jobs/                   every PySpark job, one file per pipeline step —
│                           bronze → dedup → silver DDL → merge → the two
│                           Step 4 safety proofs → Q1 audit → the three Gold
│                           reports → table maintenance
├── airflow/dags/           the Step 10 orchestration DAG — real EMR
│                           Serverless + Redshift calls, wired in the same
│                           dependency order these jobs were run in manually
├── iam/                    the trust + permissions policies actually
│                           attached to this project's IAM roles (EMR
│                           execution role, Redshift Spectrum role)
├── runbook/                a static, local mirror of the live report — full
│                           content, kept in sync manually (re-sync from the
│                           artifact if the live report changes after this)
├── sql/                    every real query, one file per brief step (1
│                           through 10, sub-steps as 4.1/4.2/6.1-6.3) —
│                           pulled from the jobs/ scripts and the Redshift/
│                           Athena queries used in Steps 8-10
├── tables/                 full CSV exports of all 7 real Iceberg tables,
│                           pulled straight from Athena — not samples
└── archive/                superseded output from an earlier, smaller-scale
                            proof — kept, not deleted, once it stopped being
                            the current approach
```

## What's real vs. what's reference code

Every script in `jobs/` was run for real against this account — the job IDs
and output quoted in the live report and `EVIDENCE.md` are from those actual
runs, not descriptions of what the code would do.

The DAG in `airflow/dags/` is real, deployable code (same operators, same
job arguments, same account IDs as the manual runs it wires together) — but
it was written *after* steps 1-9 were built and debugged interactively at
the CLI, specifically to satisfy Step 10's full-pipeline re-run requirement.
It has not been executed inside a live Airflow scheduler in this session;
the identical steps it calls were run manually instead, and those results
are what the evidence pack reports. Deploying it to `../airflow_local`
(this repo's runnable Airflow-in-Docker lab) would execute the same,
already-proven jobs.

## AWS resources this project owns

| Resource | Identifier |
|---|---|
| EMR Serverless application | `fintech-cdc-spark-app` (`00g8g90j3dqrmi09`) |
| IAM execution role (EMR) | `ali-fintech-cdc-emr-role` |
| IAM role (Redshift Spectrum) | `ali-fintech-exam-redshift-role` |
| S3 bucket | `ali-iceberg-schema-demo-275829498730` (prefix `fintech_cdc/`) |
| Glue database | `fintech_db` |
| Redshift Serverless namespace / workgroup | `ali-fintech-exam-ns` / `ali-fintech-exam-rs-wg` |

None of these are shared with any other project or teammate in this account.
