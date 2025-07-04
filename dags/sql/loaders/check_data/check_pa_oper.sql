--- проверка на сущестование новых строк в таблице pa_oper

SELECT
    1
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
		gold_layer.dim_pa_oper)
limit 1;
