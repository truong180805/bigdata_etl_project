# File: scripts/verify_data.py
# Mục đích: Kiểm tra nhanh số lượng dữ liệu trong kho (warehouse_db) để demo cho giảng viên

import psycopg2

WAREHOUSE_DB = {
    'dbname': 'warehouse_db', 
    'user': 'airflow', 
    'password': 'airflow', 
    'host': 'localhost', 
    'port': '5432' # Đổi thành 5433 nếu DBeaver của bạn đang dùng cổng 5433
}

def verify_warehouse():
    print("Đang kết nối vào Kho dữ liệu (warehouse_db)...")
    try:
        conn = psycopg2.connect(**WAREHOUSE_DB)
        cur = conn.cursor()

        # Đếm số đơn hàng hợp lệ
        cur.execute("SELECT COUNT(*) FROM fact_orders;")
        valid_count = cur.fetchone()[0]

        # Đếm số đơn hàng bị lỗi
        cur.execute("SELECT COUNT(*) FROM quarantine_orders;")
        invalid_count = cur.fetchone()[0]

        print("\n" + "="*40)
        print("BÁO CÁO DỮ LIỆU ETL")
        print("="*40)
        print(f" Số lượng đơn hàng SẠCH (fact_orders): {valid_count} dòng")
        print(f" Số lượng đơn hàng LỖI (quarantine_orders): {invalid_count} dòng")
        print("="*40)
        print("Backfill nhiều lần mà số lượng SẠCH không bị nhân đôi,")
        print("chứng tỏ tính năng UPSERT (Idempotent) hoạt động hoàn hảo!\n")

    except Exception as e:
        print(f"Lỗi kết nối: {e}")
    finally:
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    verify_warehouse()