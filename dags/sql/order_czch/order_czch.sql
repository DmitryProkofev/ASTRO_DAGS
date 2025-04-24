BEGIN;

DELETE FROM bi_data.order_szch;

INSERT INTO bi_data.order_szch 
SELECT *
FROM airflow_data.order_szch;

COMMIT;