#

CREATE TABLE bronze_layer.loaders_reasons_new engine = MergeTree
ORDER BY
id AS
SELECT
	*,
	now('Europe/Samara') AS update_etl
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_reasons',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE toUnixTimestamp(updated_at) > (
	select
		max(toUnixTimestamp(updated_at))
	from
		bronze_layer.loaders_reasons_bronze);


RENAME TABLE bronze_layer.loaders_reasons TO bronze_layer.loaders_reasons_old;


RENAME TABLE bronze_layer.loaders_reasons_new TO bronze_layer.loaders_reasons;


