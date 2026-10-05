# File: scripts/generate_data.py
# Mục đích: Sinh dữ liệu đơn hàng giả lập và nạp vào source_db

import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
import psycopg2.extras

# Khởi tạo công cụ sinh dữ liệu ngẫu nhiên
fake = Faker()

# Cấu hình kết nối DB
# Vì bạn đang chạy code trên máy tính cá nhân để test, host sẽ là 'localhost'
DB_CONFIG = {
    'dbname': 'source_db',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': '5432'
}

def generate_mock_orders(num_records=100):
    """Hàm tạo ra danh sách các đơn hàng ngẫu nhiên"""
    orders = []
    now = datetime.now()

    for _ in range(num_records):
        # 80% là dữ liệu hoàn toàn hợp lệ
        if random.random() < 0.8:
            order_id = str(uuid.uuid4())
            amount = round(random.uniform(10.0, 500.0), 2)
            # Giả lập thời gian mua hàng là từ 1 đến 60 phút trước
            event_time = now - timedelta(minutes=random.randint(1, 60))
        else:
            # 20% là dữ liệu cố tình làm lỗi để test Quarantine và Late data
            error_type = random.choice(['negative_amount', 'null_id', 'late_data'])
            
            if error_type == 'negative_amount':
                order_id = str(uuid.uuid4())
                amount = round(random.uniform(-100.0, -1.0), 2) # Cố tình cho tiền âm
                event_time = now - timedelta(minutes=random.randint(1, 60))
                
            elif error_type == 'null_id':
                order_id = None # Cố tình bỏ trống ID
                amount = round(random.uniform(10.0, 500.0), 2)
                event_time = now - timedelta(minutes=random.randint(1, 60))
                
            elif error_type == 'late_data':
                order_id = str(uuid.uuid4())
                amount = round(random.uniform(10.0, 500.0), 2)
                # Dữ liệu muộn: Khách mua từ 5 ngày trước nhưng giờ hệ thống mới nhận được
                event_time = now - timedelta(days=5)

        user_id = str(random.randint(1, 1000))
        status = random.choice(['PENDING', 'COMPLETED', 'CANCELLED'])
        updated_at = now # Thời gian bản ghi được tạo vào hệ thống luôn là hiện tại

        # Gom nhóm thành một dòng dữ liệu (Tuple)
        orders.append((order_id, user_id, amount, status, event_time, updated_at))

    return orders

def insert_to_source_db(orders):
    """Hàm kết nối DB và đẩy dữ liệu vào bảng"""
    conn = None
    try:
        # Mở kết nối đến database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Câu lệnh SQL chèn dữ liệu
        insert_query = """
            INSERT INTO orders (id, user_id, amount, status, event_time, updated_at)
            VALUES %s
        """

        # Dùng execute_values để chèn nhiều dòng cùng lúc. 
        # Điều này giúp tiết kiệm RAM và CPU hơn rất nhiều so với vòng lặp INSERT từng dòng.
        psycopg2.extras.execute_values(cur, insert_query, orders)

        # Lưu thay đổi (Commit)
        conn.commit()
        print(f"Thành công: Đã chèn {len(orders)} đơn hàng vào source_db!")

    except Exception as e:
        print(f"Thất bại: Đã xảy ra lỗi khi chèn dữ liệu - {e}")
    finally:
        # Luôn luôn nhớ đóng kết nối để giải phóng RAM
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    print("Bắt đầu sinh dữ liệu giả lập...")
    
    # Bước 1: Sinh ra 100 đơn hàng
    mock_data = generate_mock_orders(100)
    
    # Bước 2: Đẩy dữ liệu vào Database
    insert_to_source_db(mock_data)