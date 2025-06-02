--какие варики обновления данной таблицы:
-- - drop/insert
-- - insert/optimize


drop table if exists bronze_layer.pa_oper;

CREATE TABLE IF NOT EXISTS bronze_layer.pa_oper engine=MergeTree order by idwork AS
SELECT
    *,
    now('Europe/Samara') AS update_data
FROM  postgresql('10.1.11.17:5432',
	'AGRO',
	'pa_oper',
	'airflow_etl',
	'airpegas',
	'public');

