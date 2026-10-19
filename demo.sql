thêm dữ liệu demo:
INSERT INTO orders (id, user_id, amount, status, event_time, updated_at) 
VALUES 
    ('DEMO_OK_001', 'CUSTOMER_01', 250000, 'completed', NOW(), NOW()), 
    ('DEMO_BAD_001', 'CUSTOMER_02', -50000, 'completed', NOW(), NOW()), 
    ('DEMO_LATE_001', 'CUSTOMER_03', 150000, 'completed', '2026-10-01 10:00:00', NOW()) 
ON CONFLICT (id) DO NOTHING;

update dữ liệu:
UPDATE orders SET amount = 300000, updated_at = NOW() 
WHERE id = 'DEMO_OK_001';

update thành lỗi:
UPDATE orders SET amount = -100000, updated_at = NOW() 
WHERE id = 'DEMO_OK_001';

upadate ko lỗi:
UPDATE orders 
SET amount = 300000, 
    updated_at = NOW() 
WHERE id = 'DEMO_OK_001';

them don hang bo sot:
INSERT INTO orders (id, user_id, amount, status, event_time, updated_at) 
VALUES ('DEMO_BACKFILL_001', 'CUSTOMER_04', 420000, 'completed', '2026-10-02 10:00:00', '2026-10-02 10:05:00') 
ON CONFLICT (id) DO NOTHING;

lệnh backfill:
docker exec -it airflow_scheduler airflow dags trigger -c '{\"backfill\": true, \"backfill_date\": \"2026-10-02\"}' order_etl_pipeline

lệnh test retry:
docker exec -it airflow_scheduler airflow dags trigger -c '{\"test_retry\": true}' order_etl_pipeline

lệnh thêm dữ liệu:
python scripts/generate_data.py --count 10000

test:
python scripts/verify_data.py  