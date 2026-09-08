-- Step 7 (brief) — table maintenance: compaction, manifests, retention, orphans.
-- jobs/table_maintenance.py on EMR Serverless.
-- Every transaction in this dataset is otherwise unique (proven in 6.2), so
-- no ordinary MERGE ever hits the UPDATE branch and merge-on-read never
-- produces a delete file. This job first manufactures 50 real, legitimate
-- correction rows (a -$0.01 fee correction on 50 real transaction_ids,
-- dated 2026-01-21) through the real bronze -> staged -> MERGE path, then
-- runs the brief's own maintenance sequence against real delete files.

-- ---- file/delete-file counts, before and after each step -------------
SELECT content, count(*) AS files, cast(avg(file_size_in_bytes) AS bigint) AS avg_bytes
FROM glue_catalog.fintech_db.silver_ledger.files
GROUP BY content ORDER BY content;
-- content = 0 -> data files, content = 1 -> delete files
-- real result (Evidence item 13): before = 395 data / 339 delete

-- ---- apply the 50 real corrections (same production MERGE as Step 4) ---
MERGE INTO glue_catalog.fintech_db.silver_ledger t
USING staged_corrections s
   ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
              AND s.source_ts > t.event_timestamp
   THEN UPDATE SET
        t.amount = s.amount, t.status = s.status, t.currency = s.currency,
        t.event_timestamp = s.source_ts, t.source_lsn = s.source_lsn,
        t._updated_at = current_timestamp();

-- ---- 1. plain compaction, the brief's exact options ---------------------
-- REAL first error hit here (break log #2): partial-progress-enabled is a
-- binpack-only option, not valid with strategy => 'sort' —
-- IllegalArgumentException: Cannot use options [partial-progress-enabled],
-- they are not supported by the action or the rewriter SORT.
-- Fixed by dropping that option (kept below):
CALL glue_catalog.system.rewrite_data_files(
    table => 'fintech_db.silver_ledger',
    strategy => 'sort',
    sort_order => 'account_id ASC, event_timestamp DESC',
    options => map(
        'target-file-size-bytes', '134217728',
        'max-concurrent-file-group-rewrites', '4'
    ));
-- real result: 0 files rewritten, 339 delete markers survive untouched —
-- the size-based filter never looks at delete markers on a healthy-sized file.

-- ---- 2. the fix: force every file to be rewritten ------------------------
CALL glue_catalog.system.rewrite_data_files(
    table => 'fintech_db.silver_ledger',
    options => map('rewrite-all', 'true'));
-- real result (Evidence item 13-15): after = 336 data / 0 delete

-- ---- 3. manifests ---------------------------------------------------------
CALL glue_catalog.system.rewrite_manifests('fintech_db.silver_ledger');

-- ---- 4. retention: deliberate no-op right now (Evidence item 16) ----------
-- keeps day 1's and day 10's snapshots queryable for this review;
-- production policy = older_than now() - 8 months, per the brief's audit window.
CALL glue_catalog.system.expire_snapshots(
    table => 'fintech_db.silver_ledger',
    older_than => TIMESTAMP '2026-09-01 00:00:00',
    retain_last => 50,
    stream_results => true);

-- ---- 5. orphan files, dry run ---------------------------------------------
CALL glue_catalog.system.remove_orphan_files(
    table => 'fintech_db.silver_ledger', dry_run => true);
