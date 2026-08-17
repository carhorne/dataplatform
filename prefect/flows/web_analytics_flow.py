"""
web_analytics_flow.py

Prefect 2.0 flow for ingesting Adventure Works web analytics clickstream data.
Pulls from the REST API incrementally, cleans/validates, stages in Snowflake,
loads via COPY INTO, and cleans up staged files after each successful cycle.

Watermark is persisted to a local file (/tmp/watermark.txt) between runs.
The CSV written to stage contains exactly six columns matching the API fields
(renamed: timestamp -> event_timestamp). _loaded_at and _file_name are
populated automatically by Snowflake DEFAULT and COPY INTO metadata.
"""

import csv
import logging
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import snowflake.connector
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

# ---------------------------------------------------------------------------
# Standard logger (works with AND without a Prefect server running)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("web_analytics_flow")


def _get_logger():
    """
    Return the Prefect run logger when inside an active flow run,
    falling back to the standard Python logger otherwise.
    This lets the flow run cleanly both locally and inside Docker+Prefect.
    """
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except Exception:
        return log


# ---------------------------------------------------------------------------
# Configuration (all from environment variables)
# ---------------------------------------------------------------------------

API_BASE_URL    = os.getenv("API_BASE_URL", "https://is566-web-analytics-api.fly.dev")
WATERMARK_FILE  = Path(os.getenv("WATERMARK_FILE", "/tmp/watermark.txt"))

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")

RAW_SCHEMA  = "RAW_EXT"
RAW_TABLE   = "WEB_ANALYTICS_RAW"
STAGE_NAME  = "WEB_ANALYTICS_STAGE"
FULL_TABLE  = f"{RAW_SCHEMA}.{RAW_TABLE}"
FULL_STAGE  = f"@{RAW_SCHEMA}.{STAGE_NAME}"

REQUIRED_FIELDS   = ["customer_id", "product_id", "session_id", "event_timestamp"]
VALID_EVENT_TYPES = {"page_view", "click", "add_to_cart", "purchase"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_snowflake_conn():
    """Open and return a snowflake-connector-python connection."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        schema=RAW_SCHEMA,
    )


def _load_watermark() -> str | None:
    """Read the persisted watermark timestamp from disk. Returns None on first run."""
    if WATERMARK_FILE.exists():
        ts = WATERMARK_FILE.read_text().strip()
        if ts:
            return ts
    return None


def _save_watermark(ts: str) -> None:
    """Persist the latest event timestamp to disk for the next run."""
    WATERMARK_FILE.write_text(ts)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_clickstream", retries=3, retry_delay_seconds=10)
def fetch_clickstream(since: str | None) -> list[dict]:
    """
    Pull clickstream events from the REST API.

    Uses the `since` parameter for incremental extraction. On first run
    (since=None) the API defaults to the last 60 minutes.
    Retries up to 3 times with 10s delay to handle transient errors / 429s.
    """
    logger = _get_logger()
    params = {}
    if since:
        params["since"] = since
        logger.info(f"Fetching events since {since}")
    else:
        logger.info("No watermark — fetching default 60-minute window")

    try:
        response = requests.get(
            f"{API_BASE_URL}/analytics/clickstream",
            params=params,
            timeout=30,
        )

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "unknown")
            logger.warning(f"Rate limited by API. Retry-After: {retry_after}s")
            response.raise_for_status()

        response.raise_for_status()
        events = response.json()
        logger.info(f"Fetched {len(events)} events from API")
        return events

    except requests.exceptions.Timeout:
        logger.error("API request timed out after 30s")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"API HTTP error: {e}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise


@task(name="clean_and_validate")
def clean_and_validate(events: list[dict]) -> list[dict]:
    """
    Clean and validate raw API events.

    1. Rename `timestamp` -> `event_timestamp`
    2. Cast customer_id and product_id to int
    3. Drop rows with nulls in required fields
    4. Deduplicate on (customer_id, session_id, event_type, event_timestamp)
    """
    logger = _get_logger()
    logger.info(f"Starting cleaning: {len(events)} raw records")

    cleaned = []
    null_dropped = 0
    cast_errors = 0

    for event in events:
        record = {
            "customer_id":     event.get("customer_id"),
            "product_id":      event.get("product_id"),
            "session_id":      event.get("session_id"),
            "page_url":        event.get("page_url"),
            "event_type":      event.get("event_type"),
            "event_timestamp": event.get("timestamp"),
        }

        try:
            if record["customer_id"] is not None:
                record["customer_id"] = int(record["customer_id"])
            if record["product_id"] is not None:
                record["product_id"] = int(record["product_id"])
        except (ValueError, TypeError):
            logger.warning(
                f"Could not cast IDs to int for session {record.get('session_id')} — dropping"
            )
            cast_errors += 1
            continue

        missing = [f for f in REQUIRED_FIELDS if record.get(f) is None]
        if missing:
            logger.warning(f"Dropping row — missing {missing}: {record}")
            null_dropped += 1
            continue

        cleaned.append(record)

    logger.info(
        f"After filtering: {len(cleaned)} records "
        f"({null_dropped} null-dropped, {cast_errors} cast-errors)"
    )

    seen = set()
    deduped = []
    for record in cleaned:
        key = (
            record["customer_id"],
            record["session_id"],
            record["event_type"],
            record["event_timestamp"],
        )
        if key not in seen:
            seen.add(key)
            deduped.append(record)

    dupes_removed = len(cleaned) - len(deduped)
    if dupes_removed:
        logger.info(f"Removed {dupes_removed} duplicate rows")

    logger.info(f"Clean records ready to stage: {len(deduped)}")
    return deduped


@task(name="stage_to_snowflake")
def stage_to_snowflake(records: list[dict]) -> str | None:
    """
    Write records to a temp CSV and PUT it to the Snowflake internal stage.

    Returns the remote filename, or None if there was nothing to stage.
    CSV has exactly six columns — _loaded_at and _file_name are NOT written;
    Snowflake populates them via DEFAULT and COPY INTO metadata.
    """
    logger = _get_logger()

    if not records:
        logger.info("No records to stage — skipping PUT")
        return None

    ts_tag   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"web_analytics_{ts_tag}.csv"
    columns  = [
        "customer_id", "product_id", "session_id",
        "page_url", "event_type", "event_timestamp",
    ]

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline=""
    ) as f:
        local_path = f.name
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    logger.info(f"Wrote {len(records)} records to temp file {local_path}")

    try:
        conn = _get_snowflake_conn()
        cur  = conn.cursor()
        put_sql = (
            f"PUT file://{local_path} {FULL_STAGE}/{filename} "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        logger.info(f"Executing PUT to {FULL_STAGE}/{filename}")
        cur.execute(put_sql)
        result = cur.fetchone()
        logger.info(f"PUT result: {result}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"PUT to Snowflake stage failed: {e}")
        raise
    finally:
        try:
            os.unlink(local_path)
        except OSError:
            pass

    return filename


@task(name="copy_into_raw_table")
def copy_into_raw_table(staged_filename: str | None) -> dict:
    """
    COPY INTO RAW_EXT.web_analytics_raw from the staged CSV file.

    Explicitly lists the six target columns so Snowflake doesn't choke
    on the two extra columns (_loaded_at, _file_name) in the table def.
    Re-raises on failure so the caller skips stage cleanup.
    """
    logger = _get_logger()
    metrics = {
        "rows_copied":   0,
        "rows_skipped":  0,
        "status":        "skipped",
        "error_message": None,
    }

    if not staged_filename:
        logger.info("No staged file — skipping COPY INTO")
        return metrics

    copy_sql = f"""
        COPY INTO {FULL_TABLE} (
            customer_id,
            product_id,
            session_id,
            page_url,
            event_type,
            event_timestamp
        )
        FROM {FULL_STAGE}/{staged_filename}
        FILE_FORMAT = (
            TYPE                       = 'CSV'
            SKIP_HEADER                = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF                    = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL        = TRUE
        )
        ON_ERROR = 'CONTINUE'
    """

    try:
        conn  = _get_snowflake_conn()
        cur   = conn.cursor()
        start = time.time()
        logger.info(f"COPY INTO {FULL_TABLE} from {staged_filename}")
        cur.execute(copy_sql)
        rows    = cur.fetchall()
        elapsed = round(time.time() - start, 2)

        total_loaded  = 0
        total_skipped = 0
        for row in rows:
            parsed        = int(row[2]) if len(row) > 2 else 0
            loaded        = int(row[3]) if len(row) > 3 else 0
            total_loaded  += loaded
            total_skipped += (parsed - loaded)

        metrics["rows_copied"]  = total_loaded
        metrics["rows_skipped"] = total_skipped
        metrics["status"]       = "success"

        logger.info(
            f"COPY INTO complete: {total_loaded} loaded, "
            f"{total_skipped} skipped ({elapsed}s)"
        )
        cur.close()
        conn.close()

    except Exception as e:
        metrics["status"]        = "error"
        metrics["error_message"] = str(e)
        logger.error(f"COPY INTO failed: {e}")
        raise

    return metrics


@task(name="clean_stage")
def clean_stage(staged_filename: str | None) -> None:
    """
    REMOVE the staged file after a successful COPY INTO.
    Logs but does not raise on failure — a missed cleanup is non-fatal.
    """
    logger = _get_logger()

    if not staged_filename:
        logger.info("No staged file to clean — skipping REMOVE")
        return

    remove_sql = f"REMOVE {FULL_STAGE}/{staged_filename}"

    try:
        conn = _get_snowflake_conn()
        cur  = conn.cursor()
        logger.info(f"REMOVE {FULL_STAGE}/{staged_filename}")
        cur.execute(remove_sql)
        result = cur.fetchall()
        logger.info(f"REMOVE result: {result}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Stage cleanup failed (non-fatal): {e}")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="web_analytics_flow")
def web_analytics_flow():
    """
    End-to-end Prefect flow for web analytics ingestion.

    1. Load watermark from disk
    2. Fetch events from API (incremental)
    3. Clean and validate
    4. Stage to Snowflake (PUT)
    5. COPY INTO raw table
    6. Clean stage (only if COPY succeeded)
    7. Persist new watermark
    8. Log summary
    """
    logger = _get_logger()
    cycle_start = time.time()

    logger.info("=" * 60)
    logger.info("WEB ANALYTICS FLOW — CYCLE START")
    logger.info("=" * 60)

    since = _load_watermark()

    events        = fetch_clickstream(since)
    clean_records = clean_and_validate(events)
    staged_filename = stage_to_snowflake(clean_records)

    copy_metrics = {
        "rows_copied": 0, "rows_skipped": 0,
        "status": "skipped", "error_message": None,
    }
    copy_ok = False

    if staged_filename:
        try:
            copy_metrics = copy_into_raw_table(staged_filename)
            copy_ok = True
        except Exception:
            logger.error(
                "COPY INTO failed — staged files kept for retry next cycle"
            )

        if copy_ok:
            clean_stage(staged_filename)
        else:
            logger.warning(
                f"Skipping stage cleanup for {staged_filename} due to COPY failure"
            )
    else:
        logger.info("0 records to load this cycle")

    if events:
        new_watermark = max(e["timestamp"] for e in events)
        _save_watermark(new_watermark)
        logger.info(f"Watermark updated to {new_watermark}")

    elapsed = round(time.time() - cycle_start, 2)
    logger.info("=" * 60)
    logger.info(
        f"CYCLE COMPLETE ({elapsed}s)\n"
        f"  Records fetched:  {len(events)}\n"
        f"  Records cleaned:  {len(clean_records)}\n"
        f"  Rows loaded:      {copy_metrics['rows_copied']}\n"
        f"  Rows skipped:     {copy_metrics['rows_skipped']}\n"
        f"  Copy status:      {copy_metrics['status']}"
    )
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    schedule_minutes = int(os.getenv("FLOW_SCHEDULE_MINUTES", "5"))

    if schedule_minutes == 0:
        # Run once and exit — good for local testing
        web_analytics_flow()
    else:
        # Run in a simple loop rather than using serve() to avoid Prefect
        # version inconsistencies with scheduling APIs. Runs immediately on
        # startup, then sleeps for the configured interval between cycles.
        import time as _time
        interval_sec = schedule_minutes * 60
        log.info(f"Starting scheduled loop: every {schedule_minutes} minute(s)")
        while True:
            try:
                web_analytics_flow()
            except Exception as e:
                log.error(f"Flow run failed: {e}")
            log.info(f"Sleeping {schedule_minutes}m until next run...")
            _time.sleep(interval_sec)