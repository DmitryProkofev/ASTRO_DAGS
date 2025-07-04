
CREATE TABLE bronze_layer.loaders_call_priorities_new engine = MergeTree
ORDER BY
id AS
SELECT
	*,
	now('Europe/Samara') AS update_etl
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_call_priorities',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE toUnixTimestamp(updated_at) > (
	select
		coalesce(max(toUnixTimestamp(updated_at)), 0)
	from
		gold_layer.dim_loaders_call_priorities);


--RENAME TABLE bronze_layer.loaders_call_priorities TO bronze_layer.loaders_call_priorities_old;
--
--
--RENAME TABLE bronze_layer.loaders_call_priorities_new TO bronze_layer.loaders_call_priorities;

