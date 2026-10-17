
# File: scripts/generate_data.py
# Sinh dữ liệu giả lập để kiểm thử ETL Pipeline

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values


DB_CONFIG = {
    "dbname": os.getenv("SOURCE_DB_NAME", "source_db"),
    "user": os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

VALID_STATUSES = ["PENDING", "COMPLETED", "CANCELLED"]


def generate_mock_orders(
    num_records=100,
    mode="mixed",
    historical_date=None,
):
    """Sinh danh sách đơn hàng, không ghi vào database."""

    if num_records <= 0:
        raise ValueError("num_records phải lớn hơn 0")

    valid_modes = [
        "normal",
        "mixed",
        "invalid",
        "late",
        "historical",
    ]

    if mode not in valid_modes:
        raise ValueError(f"Mode không hợp lệ: {mode}")

    now = datetime.now()
    orders = []

    if mode == "historical":
        if not historical_date:
            raise ValueError(
                "historical cần tham số --date YYYY-MM-DD"
            )

        target_date = datetime.strptime(
            historical_date, "%Y-%m-%d"
        )

        if target_date.date() > now.date():
            raise ValueError(
                "Không sử dụng ngày tương lai cho historical"
            )

    # Tạo dữ liệu với số lượng chính xác theo từng loại
    if mode == "mixed":
        invalid_count = num_records // 5
        late_count = num_records // 10
        normal_count = num_records - invalid_count - late_count

        record_types = (
            ["normal"] * normal_count
            + ["invalid"] * invalid_count
            + ["late"] * late_count
        )

        random.shuffle(record_types)

    else:
        record_types = [mode] * num_records

    for record_type in record_types:

        order_id = str(uuid.uuid4())
        user_id = str(random.randint(1, 1000))
        amount = round(random.uniform(10, 500), 2)
        status = random.choice(VALID_STATUSES)

        event_time = now - timedelta(
            minutes=random.randint(1, 60)
        )
        updated_at = now

        if record_type == "invalid":
            # Lỗi có chủ đích: số tiền âm
            amount = -round(random.uniform(1, 100), 2)

        elif record_type == "late":
            # Đơn phát sinh 5 ngày trước,
            # nhưng mới cập nhật vào hệ thống hôm nay
            event_time = now - timedelta(days=5)

        elif record_type == "historical":
            # Dữ liệu thuộc ngày lịch sử để Backfill
            offset = timedelta(
                hours=random.randint(0, 22),
                minutes=random.randint(0, 59),
            )

            event_time = target_date + offset
            updated_at = event_time + timedelta(minutes=1)

        orders.append((
            order_id,
            user_id,
            amount,
            status,
            event_time,
            updated_at,
        ))

    return orders


def insert_to_source_db(orders):
    """Ghi dữ liệu vào source_db.orders."""

    if not orders:
        print("Không có dữ liệu cần chèn.")
        return 0

    conn = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            query = """
                INSERT INTO orders (
                    id,
                    user_id,
                    amount,
                    status,
                    event_time,
                    updated_at
                )
                VALUES %s
            """

            execute_values(
                cur,
                query,
                orders,
                page_size=1000,
            )

        conn.commit()

        print(
            f"Thành công: Đã chèn {len(orders)} "
            "đơn hàng vào source_db!"
        )

        return len(orders)

    except Exception:
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()


def print_summary(orders):
    """Thống kê dữ liệu đã sinh."""

    total = len(orders)

    invalid = sum(
        1 for row in orders
        if row[2] is None or row[2] < 0
    )

    late = sum(
        1 for row in orders
        if (row[5] - row[4]).total_seconds()
        >= 24 * 3600
    )

    print("\n===== DATA GENERATION SUMMARY =====")
    print(f"Total records : {total}")
    print(f"Valid records : {total - invalid}")
    print(f"Invalid records: {invalid}")
    print(f"Late records  : {late}")
    print("===================================\n")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Sinh dữ liệu kiểm thử ETL"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Số lượng bản ghi",
    )

    parser.add_argument(
        "--mode",
        choices=[
            "normal",
            "mixed",
            "invalid",
            "late",
            "historical",
        ],
        default="mixed",
    )

    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Ngày dữ liệu lịch sử YYYY-MM-DD",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ sinh và kiểm tra, không ghi database",
    )

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count phải lớn hơn 0")

    if args.mode == "historical" and not args.date:
        parser.error(
            "Chế độ historical yêu cầu --date"
        )

    orders = generate_mock_orders(
        num_records=args.count,
        mode=args.mode,
        historical_date=args.date,
    )

    print_summary(orders)

    if args.dry_run:
        print("DRY RUN: Không ghi dữ liệu vào database.")
    else:
        insert_to_source_db(orders)
