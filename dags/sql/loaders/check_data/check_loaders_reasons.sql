--- проверка на сущестование новых строк в таблице loader_reasons

SELECT
	1
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_reasons',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE toUnixTimestamp(updated_at) > (
	select
		coalesce(max(toUnixTimestamp(updated_at)), 0)
	from
		bronze_layer.loaders_reasons)
limit 1;