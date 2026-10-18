# File: scripts/etl_tasks.py
# Mục đích: Xử lý logic Extract - Transform - Load

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta

# Cấu hình kết nối DB (Dùng 'localhost' khi bạn chạy test trên máy cá nhân)
# LƯU Ý CHO THÀNH VIÊN B: Khi ghép vào Airflow (chạy trong Docker),
# hãy đổi 'localhost' thành tên service container là 'postgres' nhé.
SOURCE_DB = {'dbname': 'source_db', 'user': 'airflow', 'password': 'airflow', 'host': 'postgres', 'port': '5432'}
WAREHOUSE_DB = {'dbname': 'warehouse_db', 'user': 'airflow', 'password': 'airflow', 'host': 'postgres', 'port': '5432'}

PIPELINE_NAME = "orders_incremental_etl"


def get_checkpoint():
    """
    Đọc thời điểm cập nhật cuối cùng đã ETL thành công.
    Nếu chưa có checkpoint, trả về None để chạy lần đầu.
    """
    conn = psycopg2.connect(**WAREHOUSE_DB)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_successful_updated_at
                FROM etl_checkpoint
                WHERE pipeline_name = %s
                """,
                (PIPELINE_NAME,)
            )

            result = cur.fetchone()

            return result[0] if result else None

    finally:
        conn.close()


def update_checkpoint(last_updated_at):
    """
    Luu moc updated_at sau khi ETL thanh cong.
    Khong cho checkpoint di lui.
    """
    if last_updated_at is None:
        return

    conn = psycopg2.connect(**WAREHOUSE_DB)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_checkpoint (
                        pipeline_name,
                        last_successful_updated_at,
                        last_run_at
                    )
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (pipeline_name)
                    DO UPDATE SET
                        last_successful_updated_at =
                            GREATEST(
                                COALESCE(
                                    etl_checkpoint.last_successful_updated_at,
                                    EXCLUDED.last_successful_updated_at
                                ),
                                EXCLUDED.last_successful_updated_at
                            ),
                        last_run_at = NOW()
                    """,
                    (PIPELINE_NAME, last_updated_at)
                )
    finally:
        conn.close()


def extract_data(logical_date_str=None, backfill=False):
    """
    Incremental Extract:
    - Lần đầu: lấy toàn bộ dữ liệu.
    - Các lần sau: chỉ lấy bản ghi mới hoặc được cập nhật
      kể từ checkpoint.
    """
    checkpoint = get_checkpoint()

    print(f"Bắt đầu Extract. Checkpoint hiện tại: {checkpoint}")

    conn = psycopg2.connect(**SOURCE_DB)

    try:
        with conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        ) as cur:

            if checkpoint is None:
                query = """
                    SELECT id, user_id, amount, status,
                           event_time, updated_at
                    FROM orders
                    ORDER BY updated_at, id
                """
                cur.execute(query)

            else:
                query = """
                    SELECT id, user_id, amount, status,
                           event_time, updated_at
                    FROM orders
                    WHERE updated_at > %s
                    ORDER BY updated_at, id
                """
                cur.execute(
                    query,
                    (checkpoint - timedelta(days=1),)
                )

            raw_records = cur.fetchall()

        print(
            f"Đã trích xuất {len(raw_records)} "
            "bản ghi từ source_db."
        )

        return raw_records

    finally:
        conn.close()

def transform_data(raw_records):
    """
    Kiểm tra chất lượng dữ liệu (Data Quality).
    Phân loại thành dữ liệu hợp lệ (valid) và dữ liệu lỗi (invalid).
    """
    print("Bắt đầu Transform (Làm sạch) dữ liệu...")
    valid_data = []
    invalid_data = []

    # Lấy thời gian hiện tại làm thời điểm xử lý (processing_time)
    processing_time = datetime.now()

    for row in raw_records:
        error_reason = None

        # 1. Kiểm tra ID không được để trống (null)
        if not row['id']:
            error_reason = "Lỗi: Mã đơn hàng (ID) bị trống"

        # 2. Kiểm tra số tiền phải >= 0
        elif row['amount'] is None or float(row['amount']) < 0:
            error_reason = "Lỗi: Số tiền đơn hàng bị âm hoặc rỗng"

        # Phân loại
        if error_reason:
            # Ghi nhận dữ liệu lỗi kèm lý do và thời gian xử lý
            invalid_data.append((
                row['id'], row['user_id'], row['amount'], row['status'],
                row['event_time'], row['updated_at'], error_reason, processing_time
            ))
        else:
            # Ghi nhận dữ liệu chuẩn kèm thời gian xử lý
            valid_data.append((
                row['id'], row['user_id'], row['amount'], row['status'],
                row['event_time'], row['updated_at'], processing_time
            ))

    print(f"Đã phân loại: {len(valid_data)} hợp lệ | {len(invalid_data)} bị lỗi.")
    return valid_data, invalid_data

def load_data(valid_data, invalid_data):
    """
    Load dữ liệu vào warehouse_db.
    - UPSERT đơn hàng hợp lệ.
    - Quarantine đơn hàng không hợp lệ.
    - Rollback nếu xảy ra lỗi.
    - Luôn đóng kết nối PostgreSQL.
    """
    print("Bắt đầu Load dữ liệu vào warehouse_db...")

    conn = psycopg2.connect(**WAREHOUSE_DB)

    try:
        with conn.cursor() as cur:

            # 1. UPSERT dữ liệu hợp lệ
            if valid_data:
                upsert_query = """
                    INSERT INTO fact_orders (
                        id, user_id, amount, status,
                        event_time, updated_at, processing_time
                    )
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        event_time = EXCLUDED.event_time,
                        updated_at = EXCLUDED.updated_at,
                        processing_time = EXCLUDED.processing_time
                    WHERE EXCLUDED.updated_at > fact_orders.updated_at;
                """

                psycopg2.extras.execute_values(
                    cur,
                    upsert_query,
                    valid_data
                )

                print(
                    f"Đã xử lý {len(valid_data)} "
                    "bản ghi hợp lệ."
                )

            # 2. Đưa dữ liệu lỗi vào quarantine
            if invalid_data:
                invalid_ids = [
                    row[0]
                    for row in invalid_data
                    if row[0] is not None
                ]

                if invalid_ids:
                    cur.execute(
                        """
                        DELETE FROM quarantine_orders
                        WHERE id = ANY(%s)
                        """,
                        (invalid_ids,)
                    )

                insert_error_query = """
                    INSERT INTO quarantine_orders (
                        id, user_id, amount, status,
                        event_time, updated_at,
                        error_reason, processing_time
                    )
                    VALUES %s
                """

                psycopg2.extras.execute_values(
                    cur,
                    insert_error_query,
                    invalid_data
                )

                print(
                    f"Đã đưa {len(invalid_data)} "
                    "bản ghi lỗi vào quarantine."
                )

        # Chỉ commit sau khi toàn bộ thao tác thành công
        conn.commit()
        print("Quá trình Load hoàn tất!")

    except Exception as e:
        conn.rollback()
        print(f"Lỗi trong quá trình Load: {e}")
        raise

    finally:
        conn.close()

# Hàm gom chung 3 bước để Airflow gọi cho dễ
def run_etl_pipeline(logical_date_str):
    """
    Hàm Orchestrator chính cho kịch bản ETL.
    Thành viên B (Airflow) sẽ gọi hàm này và truyền vào ngày cần chạy.
    """
    raw_data = extract_data(logical_date_str)

    if not raw_data:
        print(f"Không có dữ liệu mới nào cho ngày {logical_date_str}. Kết thúc sớm.")
        return

    valid_data, invalid_data = transform_data(raw_data)

    # Nap du lieu vao warehouse
    load_data(valid_data, invalid_data)

    # Chi cap nhat checkpoint sau khi Load thanh cong
    timestamps = [
        row["updated_at"]
        for row in raw_data
        if row["updated_at"] is not None
    ]

    if timestamps:
        latest_updated_at = max(timestamps)
        update_checkpoint(latest_updated_at)

        print(
            f"Checkpoint da cap nhat: {latest_updated_at}"
        )
