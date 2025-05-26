insert into staging.loaders_calls
SELECT
	id,
	open_time,
	customer_id,
	workshop_id,
	call_reason_id,
	comment,
	loader_id,
	taken_time,
	close_time,
	priority,
	container_qty,
	now('Europe/Samara') AS update_ad
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loader_calls',
	'airflow_etl',
	'airpegas',
	'public')
	WHERE customer_id NOT IN (5773698501, 325813539)
  AND loader_id NOT IN (5773698501, 325813539)
and close_time is not Null
and toUnixTimestamp(close_time) > (select max(toUnixTimestamp(close_time)) from staging.loaders_calls);