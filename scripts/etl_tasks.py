# File: scripts/etl_tasks.py
# ETL don hang: incremental, backfill, quarantine, checkpoint va retry-safe load.

from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

SOURCE_DB = {
    "dbname": "source_db", "user": "airflow", "password": "airflow",
    "host": "postgres", "port": "5432",
}
WAREHOUSE_DB = {
    "dbname": "warehouse_db", "user": "airflow", "password": "airflow",
    "host": "postgres", "port": "5432",
}

PIPELINE_NAME = "orders_incremental_etl"
LOOKBACK_DAYS = 1


def get_checkpoint():
    """Doc watermark cua lan incremental thanh cong gan nhat."""
    conn = psycopg2.connect(**WAREHOUSE_DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT last_successful_updated_at
                   FROM etl_checkpoint WHERE pipeline_name = %s""",
                (PIPELINE_NAME,),
            )
            record = cur.fetchone()
            return record[0] if record else None
    finally:
        conn.close()


def extract_data(logical_date_str=None, backfill=False):
    """Incremental theo updated_at; backfill theo ngay event_time.

    Incremental: doc lai LOOKBACK_DAYS ngay de xu ly late updates.
    Backfill: khong dung checkpoint; chi lay su kien trong ngay yeu cau.
    """
    conn = psycopg2.connect(**SOURCE_DB)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            columns = "id, user_id, amount, status, event_time, updated_at"
            if backfill:
                if not logical_date_str:
                    raise ValueError("Backfill can ngay YYYY-MM-DD (logical_date_str).")
                start = datetime.strptime(logical_date_str, "%Y-%m-%d")
                end = start + timedelta(days=1)
                cur.execute(
                    f"""SELECT {columns} FROM orders
                        WHERE event_time >= %s AND event_time < %s
                        ORDER BY updated_at, id""",
                    (start, end),
                )
                print(f"BACKFILL: ngay su kien {logical_date_str}; khong doi checkpoint.")
            else:
                checkpoint = get_checkpoint()
                if checkpoint is None:
                    cur.execute(
                        f"SELECT {columns} FROM orders ORDER BY updated_at, id"
                    )
                    print("INCREMENTAL: chua co checkpoint; nap du lieu ban dau.")
                else:
                    lookback_start = checkpoint - timedelta(days=LOOKBACK_DAYS)
                    cur.execute(
                        f"""SELECT {columns} FROM orders
                            WHERE updated_at >= %s
                            OR updated_at IS NULL
                            ORDER BY updated_at, id""",
                        (lookback_start,),
                    )
                    print(f"INCREMENTAL: checkpoint={checkpoint}; tu {lookback_start}.")
            records = cur.fetchall()
        print(f"Extract: {len(records)} ban ghi.")
        return records
    finally:
        conn.close()


def transform_data(raw_records):
    """Tach don hang hop le va khong hop le."""
    processing_time = datetime.now()
    valid_data, invalid_data = [], []

    for row in raw_records:
        reason = None
        if not row["id"]:
            reason = "Loi: Ma don hang (ID) bi trong"
        elif row["amount"] is None or row["amount"] < 0:
            reason = "Loi: So tien don hang bi am hoac rong"
        elif row["updated_at"] is None:
            reason = "Loi: updated_at bi rong"

        common = (
            row["id"], row["user_id"], row["amount"], row["status"],
            row["event_time"], row["updated_at"],
        )
        if reason:
            invalid_data.append(common + (reason, processing_time))
        else:
            valid_data.append(common + (processing_time,))

    print(f"Transform: {len(valid_data)} hop le; {len(invalid_data)} loi.")
    return valid_data, invalid_data



def load_data(valid_data, invalid_data, checkpoint_value=None, simulate_failure=False):
    """
    Dong bo trang thai don hang moi nhat.
    - Ban hop le moi hon: UPSERT fact, xoa quarantine cu.
    - Ban loi moi hon: xoa fact cu, ghi quarantine.
    - Backfill cu khong duoc ghi de trang thai moi hon.
    - Load va checkpoint chung mot transaction.
    """
    conn = psycopg2.connect(**WAREHOUSE_DB)

    try:
        with conn.cursor() as cur:
            accepted_valid = []
            accepted_invalid = []

            # Kiem tra phien ban dang luu trong ca hai bang
            incoming = valid_data + invalid_data
            ids = list({row[0] for row in incoming if row[0] is not None})
            latest_by_id = {}

            if ids:
                cur.execute(
                    """
                    SELECT id, MAX(updated_at)
                    FROM (
                        SELECT id, updated_at
                        FROM fact_orders
                        WHERE id = ANY(%s)
                        UNION ALL
                        SELECT id, updated_at
                        FROM quarantine_orders
                        WHERE id = ANY(%s)
                    ) versions
                    GROUP BY id
                    """,
                    (ids, ids),
                )
                latest_by_id = dict(cur.fetchall())

            for row in valid_data:
                old_time = latest_by_id.get(row[0])
                if old_time is None or (
                    row[5] is not None and row[5] > old_time
                ):
                    accepted_valid.append(row)

            for row in invalid_data:
                old_time = latest_by_id.get(row[0])
                if row[0] is None or old_time is None or (
                    row[5] is not None and row[5] > old_time
                ):
                    accepted_invalid.append(row)

            # 1. Xu ly phien ban hop le moi hon
            if accepted_valid:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO fact_orders
                    (id, user_id, amount, status,
                     event_time, updated_at, processing_time)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        event_time = EXCLUDED.event_time,
                        updated_at = EXCLUDED.updated_at,
                        processing_time = EXCLUDED.processing_time
                    WHERE fact_orders.updated_at IS NULL
                       OR EXCLUDED.updated_at > fact_orders.updated_at
                    """,
                    accepted_valid,
                )

                cur.execute(
                    "DELETE FROM quarantine_orders WHERE id = ANY(%s)",
                    ([row[0] for row in accepted_valid],),
                )

            # 2. Xu ly phien ban loi moi hon
            if accepted_invalid:
                invalid_ids = [
                    row[0] for row in accepted_invalid
                    if row[0] is not None
                ]

                if invalid_ids:
                    # Phuong an A: khong giu fact cu khi ban moi loi
                    cur.execute(
                        "DELETE FROM fact_orders WHERE id = ANY(%s)",
                        (invalid_ids,),
                    )
                    cur.execute(
                        "DELETE FROM quarantine_orders WHERE id = ANY(%s)",
                        (invalid_ids,),
                    )

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO quarantine_orders
                    (id, user_id, amount, status, event_time,
                     updated_at, error_reason, processing_time)
                    VALUES %s
                    """,
                    accepted_invalid,
                )

            # 3. Cap nhat checkpoint cung transaction
            if checkpoint_value is not None:
                cur.execute(
                    """
                    INSERT INTO etl_checkpoint
                    (pipeline_name, last_successful_updated_at, last_run_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        last_successful_updated_at = GREATEST(
                            COALESCE(
                                etl_checkpoint.last_successful_updated_at,
                                EXCLUDED.last_successful_updated_at
                            ),
                            EXCLUDED.last_successful_updated_at
                        ),
                        last_run_at = NOW()
                    """,
                    (PIPELINE_NAME, checkpoint_value),
                )

            # Giu lai che do thu nghiem Retry
            if simulate_failure:
                raise RuntimeError(
                    "SIMULATED_FAILURE: Kiem tra rollback va Airflow retry"
                )

        conn.commit()
        print(
            f"Load thanh cong: {len(accepted_valid)} hop le moi; "
            f"{len(accepted_invalid)} ban loi moi."
        )

    except Exception as exc:
        conn.rollback()
        print(f"ETL Load loi; da rollback: {exc}")
        raise

    finally:
        conn.close()

def run_etl_pipeline(
    logical_date_str=None,
    backfill=False,
    test_retry=False,
    attempt_number=1
):
    """Chay ETL, ho tro incremental, backfill va kiem thu retry."""

    attempt_number = int(attempt_number)

    raw_records = extract_data(logical_date_str, backfill=backfill)

    if not raw_records:
        if test_retry:
            raise RuntimeError("Khong co du lieu de kiem thu Retry")
        print("Khong co du lieu phu hop, ket thuc.")
        return {
            "extracted": 0,
            "valid": 0,
            "invalid": 0,
            "backfill": backfill
        }

    valid_data, invalid_data = transform_data(raw_records)

    latest_updated_at = None
    if not backfill:
        times = [
            row["updated_at"]
            for row in raw_records
            if row["updated_at"] is not None
        ]
        if times:
            latest_updated_at = max(times)

    # Chi gia lap loi o lan thu nhat
    simulate_failure = bool(test_retry) and attempt_number == 1

    print(f"Airflow attempt: {attempt_number}")
    print(f"Simulate failure: {simulate_failure}")

    load_data(
        valid_data,
        invalid_data,
        checkpoint_value=latest_updated_at,
        simulate_failure=simulate_failure
    )

    return {
        "extracted": len(raw_records),
        "valid": len(valid_data),
        "invalid": len(invalid_data),
        "backfill": backfill
    }
