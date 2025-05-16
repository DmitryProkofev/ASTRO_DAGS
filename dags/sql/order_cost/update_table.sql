BEGIN;

DELETE FROM bi_data.order_cost_copy;

INSERT INTO bi_data.order_cost_copy
SELECT *, NOW() FROM airflow_data.order_cost;

COMMIT;
