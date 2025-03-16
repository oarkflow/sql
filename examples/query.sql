WITH
data AS (SELECT * FROM read_service('test_db.charge_master') WHERE work_item_id = 37 LIMIT 10),
data1 AS (SELECT * FROM read_service('test_db.charge_master') WHERE work_item_id = 37 LIMIT 10)
SELECT * FROM data JOIN data1 ON data.cpt_hcpcs_code = data1.cpt_hcpcs_code;
