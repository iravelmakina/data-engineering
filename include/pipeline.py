"""
Business logic for the support call enrichment pipeline.
Public functions (detect_new_calls, load_telephony_details,
transform_and_load_duckdb) are Airflow task callables.
Private helpers (_read_telephony_json, _fetch_employees, etc.)
encapsulate reusable logic.
"""

import json
import logging
import os

DUCKDB_PATH = os.environ["DUCKDB_PATH"]
TELEPHONY_DIR = os.environ["TELEPHONY_DIR"]
STAGING_DIR = os.environ["STAGING_DIR"]
WATERMARK_VAR = "last_loaded_call_time"
MYSQL_CONN_ID = "mysql_support"

logger = logging.getLogger(__name__)


def detect_new_calls(**context):
    from airflow.models import Variable
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    watermark = Variable.get(WATERMARK_VAR, default_var="1970-01-01 00:00:00")
    logger.info(f"Current watermark: {watermark}")

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    columns = ("call_id", "employee_id", "call_time", "phone", "direction", "status")
    rows = hook.get_records(
        "SELECT call_id, employee_id, call_time, phone, direction, status "
        "FROM calls WHERE call_time > %s ORDER BY call_time",
        parameters=(watermark,),
    )

    calls = [dict(zip(columns, row)) for row in rows]
    for call in calls:
        call["call_time"] = call["call_time"].strftime("%Y-%m-%d %H:%M:%S")

    call_ids = [c["call_id"] for c in calls]
    logger.info(f"Detected {len(call_ids)} new calls.")

    # Write to staging file instead of pushing large data via XCom
    run_id = context["run_id"].replace(":", "_")
    os.makedirs(STAGING_DIR, exist_ok=True)
    staging_path = os.path.join(STAGING_DIR, f"new_calls_{run_id}.json")
    with open(staging_path, "w") as f:
        json.dump(calls, f)

    context["ti"].xcom_push(key="calls_path", value=staging_path)
    context["ti"].xcom_push(key="new_call_ids", value=call_ids)


def _read_telephony_json(call_id):
    """Read and validate a single telephony JSON file. Returns record or None."""
    file_path = os.path.join(TELEPHONY_DIR, f"call_{call_id}.json")

    if not os.path.exists(file_path):
        logger.warning(f"JSON file not found for call_id={call_id}")
        return None

    try:
        with open(file_path, "r") as f:
            record = json.load(f)
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON for call_id={call_id}")
        return None

    if not all(k in record for k in ("call_id", "duration_sec", "short_description")):
        logger.warning(f"Missing fields in JSON for call_id={call_id}")
        return None

    if record["duration_sec"] < 0:
        logger.warning(f"Negative duration for call_id={call_id}")
        return None

    return record


def load_telephony_details(**context):
    call_ids = context["ti"].xcom_pull(task_ids="detect_new_calls", key="new_call_ids")

    if not call_ids:
        logger.info("No new call IDs, skipping telephony load.")
        context["ti"].xcom_push(key="telephony_path", value=None)
        return

    telephony_records = []
    rejected = 0

    for call_id in call_ids:
        record = _read_telephony_json(call_id)
        if record:
            telephony_records.append(record)
        else:
            rejected += 1

    logger.info(f"Loaded {len(telephony_records)} telephony records, rejected {rejected}.")

    # Write to staging file instead of XCom
    run_id = context["run_id"].replace(":", "_")
    staging_path = os.path.join(STAGING_DIR, f"telephony_{run_id}.json")
    with open(staging_path, "w") as f:
        json.dump(telephony_records, f)

    context["ti"].xcom_push(key="telephony_path", value=staging_path)


def _fetch_employees():
    """Fetch all employees from MySQL and return as a dict keyed by employee_id."""
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    columns = ("employee_id", "full_name", "team", "role", "hire_date")
    rows = hook.get_records(
        "SELECT employee_id, full_name, team, role, hire_date FROM employees"
    )

    employees = [dict(zip(columns, row)) for row in rows]
    for emp in employees:
        emp["hire_date"] = emp["hire_date"].strftime("%Y-%m-%d")

    return {e["employee_id"]: e for e in employees}


def _enrich_calls(calls, telephony, emp_lookup):
    """Join calls with telephony and employee data. Returns list of enriched tuples."""
    tel_lookup = {t["call_id"]: t for t in telephony}

    enriched = []
    for call in calls:
        emp = emp_lookup.get(call["employee_id"])
        tel = tel_lookup.get(call["call_id"])

        if not emp:
            logger.warning(f"Employee not found for call_id={call['call_id']}")
            continue

        enriched.append((
            call["call_id"],
            call["employee_id"],
            emp["full_name"],
            emp["team"],
            emp["role"],
            call["call_time"],
            call["phone"],
            call["direction"],
            call["status"],
            tel["duration_sec"] if tel else None,
            tel["short_description"] if tel else None,
        ))

    logger.info(f"Enriched {len(enriched)} records.")
    return enriched


def _load_to_duckdb(enriched):
    """Create table if needed and upsert enriched records into DuckDB."""
    import duckdb

    db = duckdb.connect(DUCKDB_PATH)
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS support_call_enriched (
                call_id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                full_name VARCHAR,
                team VARCHAR,
                role VARCHAR,
                call_time TIMESTAMP,
                phone VARCHAR,
                direction VARCHAR,
                status VARCHAR,
                duration_sec INTEGER,
                short_description VARCHAR
            )
        """)

        call_ids = [r[0] for r in enriched]
        if call_ids:
            db.execute(
                "DELETE FROM support_call_enriched WHERE call_id IN (SELECT UNNEST($1::INTEGER[]))",
                [call_ids],
            )

        db.executemany(
            "INSERT INTO support_call_enriched VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            enriched,
        )
    finally:
        db.close()

    logger.info(f"Loaded {len(enriched)} rows into DuckDB.")


def _cleanup_staging(*paths):
    """Remove staging files if they exist."""
    for path in paths:
        if path and os.path.exists(path):
            os.remove(path)
            logger.info(f"Cleaned up staging file: {path}")


def transform_and_load_duckdb(**context):
    from airflow.models import Variable

    calls_path = context["ti"].xcom_pull(task_ids="detect_new_calls", key="calls_path")
    telephony_path = context["ti"].xcom_pull(task_ids="load_telephony_details", key="telephony_path")

    try:
        with open(calls_path, "r") as f:
            calls = json.load(f)

        if not calls:
            logger.info("No new calls to load.")
            return

        telephony = []
        if telephony_path:
            with open(telephony_path, "r") as f:
                telephony = json.load(f)

        emp_lookup = _fetch_employees()
        enriched = _enrich_calls(calls, telephony, emp_lookup)

        if enriched:
            _load_to_duckdb(enriched)
            # Advance watermark only up to the latest *enriched* record,
            # so calls that failed enrichment can be retried next run.
            max_call_time = max(r[5] for r in enriched)  # index 5 = call_time
            Variable.set(WATERMARK_VAR, max_call_time)
            logger.info(f"Watermark updated to {max_call_time}.")
        else:
            logger.warning("No records were enriched — watermark not advanced.")

    finally:
        _cleanup_staging(calls_path, telephony_path)
