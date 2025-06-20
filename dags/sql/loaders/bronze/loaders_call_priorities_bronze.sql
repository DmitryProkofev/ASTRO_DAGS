#

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
		max(toUnixTimestamp(updated_at))
	from
		bronze_layer.loaders_call_priorities_bronze);


RENAME TABLE bronze_layer.loaders_call_priorities TO bronze_layer.loaders_call_priorities_old;


RENAME TABLE bronze_layer.loaders_call_priorities_new TO bronze_layer.loaders_call_priorities;

