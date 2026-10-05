-- File: sql/init_warehouse.sql
-- Mục đích: Tạo kho dữ liệu sạch và bảng chứa lỗi trong database warehouse_db

-- 1. Bảng fact_orders (Dữ liệu Sạch)
CREATE TABLE IF NOT EXISTS fact_orders (
    id VARCHAR(50) PRIMARY KEY,       -- Phải có Primary Key để làm UPSERT (Cập nhật nếu trùng)
    user_id VARCHAR(50),
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    event_time TIMESTAMP,
    updated_at TIMESTAMP,
    processing_time TIMESTAMP         -- Thời gian pipeline ETL nạp dữ liệu này vào kho
);

-- Tạo index để truy vấn phân tích theo thời gian nhanh hơn
CREATE INDEX idx_fact_orders_event_time ON fact_orders(event_time);

-- 2. Bảng quarantine_orders (Dữ liệu Lỗi / Dead-Letter Queue)
-- Bảng này không nên set Primary Key cho cột id, vì dữ liệu lỗi có thể bị trùng id hoặc id bị NULL
CREATE TABLE IF NOT EXISTS quarantine_orders (
    id VARCHAR(50),                   -- Cố tình không để Primary Key
    user_id VARCHAR(50),
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    event_time TIMESTAMP,
    updated_at TIMESTAMP,
    error_reason TEXT,                -- Lý do bị từ chối (VD: 'Negative amount', 'Null ID')
    processing_time TIMESTAMP         -- Thời gian phát hiện lỗi
);