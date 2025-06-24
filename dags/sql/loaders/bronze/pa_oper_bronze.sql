
CREATE TABLE IF NOT EXISTS bronze_layer.pa_oper_new engine=MergeTree order by idwork AS
SELECT
    *,
    now('Europe/Samara') AS update_etl
FROM  postgresql('10.1.11.17:5432',
	'AGRO',
	'pa_oper',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE toUnixTimestamp(dtmodified) > (
	select
		coalesce(max(toUnixTimestamp(dtmodified)), 0)
	from
		bronze_layer.pa_oper);


--RENAME TABLE bronze_layer.pa_oper TO bronze_layer.pa_oper_old;
--
--
--RENAME TABLE bronze_layer.pa_oper_new TO bronze_layer.pa_oper;



