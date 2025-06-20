#

CREATE TABLE bronze_layer.loaders_workshops_new engine = MergeTree
ORDER BY
id AS
SELECT
	*,
	now('Europe/Samara') AS update_etl
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_workshops',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE toUnixTimestamp(updated_at) > (
	select
		max(toUnixTimestamp(updated_at))
	from
		bronze_layer.loaders_workshops_bronze); 
		#TODO дату брать по таблице измерений в этом запросе, хотя с другой стороны зачем, если есть предыдущая версия загрузки, от которой мы можем отталкиваться


RENAME TABLE bronze_layer.loaders_workshops TO bronze_layer.loaders_workshops_old;


RENAME TABLE bronze_layer.loaders_workshops_new TO bronze_layer.loaders_workshops;

