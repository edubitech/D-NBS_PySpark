-- Step 8 (brief) — Redshift integration, zero-copy via Spectrum.
-- Engine: Redshift Serverless (workgroup ali-fintech-exam-rs-wg, database
-- fintech, namespace ali-fintech-exam-ns), run via the Query Editor v2 /
-- Data API — not a Spark job. Points at the existing Glue catalog; Redshift
-- reads the same S3/Iceberg files every earlier step already wrote to,
-- copying zero bytes into Redshift's own storage.

CREATE EXTERNAL SCHEMA spectrum_fintech
FROM DATA CATALOG
DATABASE 'fintech_db'
IAM_ROLE 'arn:aws:iam::275829498730:role/ali-fintech-exam-redshift-role'
REGION 'us-east-1';

-- ali-fintech-exam-redshift-role's policy (iam/redshift-access-policy.json)
-- grants only glue:Get* on fintech_db + s3:GetObject/ListBucket on the one
-- warehouse bucket/prefix — scoped to this project, nothing else in the account.

-- Only the four Gold tables are exposed this way — Spectrum never reaches
-- into bronze or silver directly, by design.
SELECT count(*) FROM spectrum_fintech.gold_daily_account_summary;
-- real result: 26,412 — exact match to the table's known row count,
-- proving this reads the real Iceberg files, not a copy.
