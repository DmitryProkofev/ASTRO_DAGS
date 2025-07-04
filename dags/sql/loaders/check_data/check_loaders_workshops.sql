-- проверка на сущестование новых строк в таблице loaders_workhops


SELECT
	1
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_workshops',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE toUnixTimestamp(updated_at) > (
	select
		coalesce(max(toUnixTimestamp(updated_at)), 0)
	from
		gold_layer.dim_loaders_workshops)
limit 1;