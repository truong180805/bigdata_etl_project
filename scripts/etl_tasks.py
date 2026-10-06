# File: scripts/etl_tasks.py
# Mục đích: Xử lý logic Extract - Transform - Load

import psycopg2
import psycopg2.extras
from datetime import datetime

# Cấu hình kết nối DB (Dùng 'localhost' khi bạn chạy test trên máy cá nhân)
# LƯU Ý CHO THÀNH VIÊN B: Khi ghép vào Airflow (chạy trong Docker), 
# hãy đổi 'localhost' thành tên service container là 'postgres' nhé.
SOURCE_DB = {'dbname': 'source_db', 'user': 'airflow', 'password': 'airflow', 'host': 'postgres', 'port': '5432'}
WAREHOUSE_DB = {'dbname': 'warehouse_db', 'user': 'airflow', 'password': 'airflow', 'host': 'postgres', 'port': '5432'}

def extract_data(logical_date_str):
    """
    Trích xuất dữ liệu tăng dần (Incremental). 
    Chỉ lấy những đơn hàng có ngày cập nhật (updated_at) trùng với logical_date.
    """
    print(f"Bắt đầu Extract dữ liệu cho ngày: {logical_date_str}")
    conn = psycopg2.connect(**SOURCE_DB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Ép kiểu updated_at về dạng DATE để so sánh với chuỗi YYYY-MM-DD
    query = """
        SELECT id, user_id, amount, status, event_time, updated_at 
        FROM orders 
        WHERE DATE(updated_at) = %s
    """
    cur.execute(query, (logical_date_str,))
    raw_records = cur.fetchall()
    
    cur.close()
    conn.close()
    
    print(f"Đã trích xuất {len(raw_records)} bản ghi từ source_db.")
    return raw_records

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
    Nạp dữ liệu vào warehouse_db.
    Sử dụng kỹ thuật UPSERT cho dữ liệu hợp lệ.
    """
    print("Bắt đầu Load dữ liệu vào warehouse_db...")
    conn = psycopg2.connect(**WAREHOUSE_DB)
    cur = conn.cursor()

    # 1. Nạp dữ liệu hợp lệ vào fact_orders (Dùng UPSERT)
    if valid_data:
        # Cú pháp ON CONFLICT của PostgreSQL giúp thực hiện UPSERT
        # Nếu id đã tồn tại, nó tự động ghi đè các cột khác bằng dữ liệu mới nhất (EXCLUDED...)
        upsert_query = """
            INSERT INTO fact_orders (id, user_id, amount, status, event_time, updated_at, processing_time)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET 
                user_id = EXCLUDED.user_id,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                event_time = EXCLUDED.event_time,
                updated_at = EXCLUDED.updated_at,
                processing_time = EXCLUDED.processing_time;
        """
        psycopg2.extras.execute_values(cur, upsert_query, valid_data)
        print(f"Đã nạp {len(valid_data)} bản ghi hợp lệ (UPSERT).")

    # 2. Nạp dữ liệu lỗi vào quarantine_orders
    if invalid_data:
        # Xóa các bản ghi lỗi cũ có cùng id để tránh bị trùng khi chạy lại DAG
        invalid_ids = [row[0] for row in invalid_data if row[0] is not None]

        if invalid_ids:
            cur.execute(
                "DELETE FROM quarantine_orders WHERE id = ANY(%s)",
                (invalid_ids,)
            )

        insert_error_query = """
            INSERT INTO quarantine_orders
            (id, user_id, amount, status, event_time, updated_at, error_reason, processing_time)
            VALUES %s
        """

        psycopg2.extras.execute_values(
            cur,
            insert_error_query,
            invalid_data
        )

        print(f"Đã nạp {len(invalid_data)} bản ghi lỗi vào khu vực cách ly.")

    conn.commit()
    cur.close()
    conn.close()
    print("Quá trình Load hoàn tất!")

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
    load_data(valid_data, invalid_data)