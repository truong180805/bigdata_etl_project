-- File: sql/init_source.sql
-- Mục đích: Tạo bảng orders trong database source_db

-- Tạo bảng orders
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(50) PRIMARY KEY,       -- Mã đơn hàng
    user_id VARCHAR(50),              -- Mã khách hàng
    amount DECIMAL(10, 2),            -- Số tiền đơn hàng
    status VARCHAR(20),               -- Trạng thái (PENDING, COMPLETED, CANCELLED)
    event_time TIMESTAMP,             -- Thời gian khách hàng bấm nút mua
    updated_at TIMESTAMP              -- Thời gian trạng thái đơn hàng bị thay đổi (dùng cho Incremental Load)
);

-- Tạo một index (mục lục) trên cột updated_at để sau này truy vấn Incremental (chỉ lấy dữ liệu mới) được nhanh hơn, không tốn RAM
CREATE INDEX idx_orders_updated_at ON orders(updated_at);