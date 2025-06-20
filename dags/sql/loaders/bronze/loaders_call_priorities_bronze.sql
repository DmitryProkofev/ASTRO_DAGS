#

CREATE TABLE bronze_layer.loaders_call_priorities_bronze_new engine = MergeTree
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


RENAME TABLE bronze_layer.loaders_call_priorities_bronze TO bronze_layer.loaders_call_priorities_bronze_old;


RENAME TABLE bronze_layer.loaders_call_priorities_bronze_new TO bronze_layer.loaders_call_priorities_bronze;

#TODO далее сразу в gold_layer с необходимыми полями как таблицу измерений