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


def load_data(valid_data, invalid_data, checkpoint_value=None):
    """Mot transaction cho fact, quarantine VA incremental checkpoint.

    Khi bat ky thao tac nao that bai: rollback toan bo va raise de Airflow retry.
    """
    conn = psycopg2.connect(**WAREHOUSE_DB)
    try:
        with conn.cursor() as cur:
            if valid_data:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO fact_orders
                       (id, user_id, amount, status, event_time, updated_at, processing_time)
                       VALUES %s
                       ON CONFLICT (id) DO UPDATE SET
                           user_id = EXCLUDED.user_id,
                           amount = EXCLUDED.amount,
                           status = EXCLUDED.status,
                           event_time = EXCLUDED.event_time,
                           updated_at = EXCLUDED.updated_at,
                           processing_time = EXCLUDED.processing_time
                       WHERE EXCLUDED.updated_at > fact_orders.updated_at""",
                    valid_data,
                )
                # Don hang da duoc sua hop le thi khong con nam trong quarantine.
                cur.execute(
                    "DELETE FROM quarantine_orders WHERE id = ANY(%s)",
                    ([item[0] for item in valid_data],),
                )

            if invalid_data:
                invalid_ids = [item[0] for item in invalid_data if item[0] is not None]
                if invalid_ids:
                    cur.execute(
                        "DELETE FROM quarantine_orders WHERE id = ANY(%s)",
                        (invalid_ids,),
                    )
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO quarantine_orders
                       (id, user_id, amount, status, event_time, updated_at,
                        error_reason, processing_time)
                       VALUES %s""",
                    invalid_data,
                )

            if checkpoint_value is not None:
                cur.execute(
                    """INSERT INTO etl_checkpoint
                       (pipeline_name, last_successful_updated_at, last_run_at)
                       VALUES (%s, %s, NOW())
                       ON CONFLICT (pipeline_name) DO UPDATE SET
                           last_successful_updated_at = GREATEST(
                               COALESCE(etl_checkpoint.last_successful_updated_at,
                                        EXCLUDED.last_successful_updated_at),
                               EXCLUDED.last_successful_updated_at
                           ),
                           last_run_at = NOW()""",
                    (PIPELINE_NAME, checkpoint_value),
                )

        conn.commit()
        print("Load va checkpoint (neu co) da commit thanh cong.")
    except Exception as exc:
        conn.rollback()
        print(f"ETL Load loi; da rollback: {exc}")
        raise
    finally:
        conn.close()


def run_etl_pipeline(logical_date_str=None, backfill=False):
    """Duong chay cho Airflow.

    backfill=False: incremental, cap nhat checkpoint.
    backfill=True: tai xu ly event_time trong logical_date_str, KHONG doi checkpoint.
    """
    raw_records = extract_data(logical_date_str, backfill=backfill)
    if not raw_records:
        print("Khong co du lieu phu hop, ket thuc.")
        return {"extracted": 0, "valid": 0, "invalid": 0, "backfill": backfill}

    valid_data, invalid_data = transform_data(raw_records)
    latest_updated_at = None
    if not backfill:
        times = [row["updated_at"] for row in raw_records if row["updated_at"] is not None]
        if times:
            latest_updated_at = max(times)

    load_data(valid_data, invalid_data, checkpoint_value=latest_updated_at)
    return {
        "extracted": len(raw_records),
        "valid": len(valid_data),
        "invalid": len(invalid_data),
        "backfill": backfill,
    }
