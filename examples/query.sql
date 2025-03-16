WITH
data AS (SELECT * FROM read_service('test_db.charge_master') WHERE work_item_id = 37 LIMIT 10)
SELECT * FROM data;
