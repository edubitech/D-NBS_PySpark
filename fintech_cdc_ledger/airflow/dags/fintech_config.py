"""
fintech_config.py — one place for every real account id, ARN, bucket and
tuning knob used by the fintech CDC ledger DAG.

Unlike 3_labs/airflow_enterprise/telco_config.py (which is deliberately all
placeholders, written to be read rather than run), everything below is a
REAL identifier in this account, verified working by direct EMR Serverless
CLI runs before this DAG was written — same pattern the runbook documents
throughout: measure it for real, then write it down.

Every value is read from an environment variable with a literal (real)
default, not from an Airflow Variable — Variable.get() at DAG-parse time is
a network call repeated on every scheduler re-parse; see telco_config.py's
docstring for the full argument. The pattern is copied here deliberately so
this DAG reads the same way as the rest of the repo's Airflow code.
"""
from __future__ import annotations

import os


def cfg(name: str, default: str) -> str:
    return os.environ.get(name.upper(), default)


# ---------------------------------------------------------------- account
ACCOUNT_ID = "275829498730"
REGION = cfg("fintech_region", "us-east-1")

# ---------------------------------------------------------------- storage
LAKE_BUCKET = cfg("fintech_lake_bucket", "ali-iceberg-schema-demo-275829498730")
CODE_PREFIX = f"s3://{LAKE_BUCKET}/fintech_cdc/scripts"
LOG_PREFIX = f"s3://{LAKE_BUCKET}/fintech_cdc/emr-logs/"
RAW_CDC_PREFIX = f"s3://{LAKE_BUCKET}/fintech_cdc/raw/cdc"
ATHENA_RESULTS = f"s3://{LAKE_BUCKET}/fintech_cdc/athena-results/"

GLUE_DB = "fintech_db"

# ---------------------------------------------------------------- compute
EMR_SERVERLESS_APP_ID = cfg("fintech_emrs_app_id", "00g8g90j3dqrmi09")
EXEC_ROLE = cfg(
    "fintech_emr_exec_role",
    f"arn:aws:iam::{ACCOUNT_ID}:role/ali-fintech-cdc-emr-role",
)

# ---------------------------------------------------------------- warehouse
REDSHIFT_WORKGROUP = cfg("fintech_redshift_workgroup", "ali-fintech-exam-rs-wg")
REDSHIFT_DB = "fintech"
ATHENA_WORKGROUP = cfg("fintech_athena_workgroup", "primary")

# ---------------------------------------------------------------- pipeline window
# The harness's own anchor: step 1 = this instant. The 10 numbered brief days
# run 2026-01-01 .. 2026-01-20; the maintenance step's correction batch adds
# one more day (2026-01-21) outside that range, on purpose — see table_maintenance.py.
PIPELINE_START_DATE = "2026-01-01"
PIPELINE_END_DATE = "2026-01-20"
IDEMPOTENCY_CHECK_TABLE = f"{GLUE_DB}.gold_daily_account_summary"

# ---------------------------------------------------------------- pools
POOL_EMR_SERVERLESS = "emr_serverless"
POOL_REDSHIFT = "redshift_serving"
POOL_ATHENA = "athena_queries"


def spark_submit(entry: str, args: list[str] | None = None) -> dict:
    """job_driver for EmrServerlessStartJobOperator, Iceberg wired in.

    Matches exactly the --conf set every job in jobs/ was actually run with
    from the CLI during this project — see any "How to verify" block in the
    published report for a job id that used these same four --conf flags.
    """
    conf = (
        "--conf spark.sql.extensions="
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
        "--conf spark.sql.catalog.glue_catalog.catalog-impl="
        "org.apache.iceberg.aws.glue.GlueCatalog "
        f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{LAKE_BUCKET}/fintech_cdc/warehouse "
        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    )
    driver = {
        "sparkSubmit": {
            "entryPoint": f"{CODE_PREFIX}/{entry}",
            "sparkSubmitParameters": conf,
        }
    }
    if args:
        driver["sparkSubmit"]["entryPointArguments"] = args
    return driver


def emrs_monitoring() -> dict:
    """configuration_overrides for EMR Serverless — S3-only logs.

    Matches every manual CLI run in this project: CloudWatch logging was
    never enabled for this app (no need to touch another lab's log group,
    the amp-ali-dlh-scripts jar mistake from the fintech runbook's own
    Step 2 break log), S3 monitoring covers every job's stdout/stderr.
    """
    return {"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": LOG_PREFIX}}}


DEFAULT_ARGS = {
    "owner": "ali.akram",
    "retries": 1,
    "retry_delay": __import__("datetime").timedelta(minutes=2),
    "depends_on_past": False,
}
