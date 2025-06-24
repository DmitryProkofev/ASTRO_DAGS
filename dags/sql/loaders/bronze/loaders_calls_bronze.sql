
--используем инкрементальную загрузку
create table bronze_layer.loaders_calls_new engine = MergeTree
order by
id as
---insert into bronze_layer.loaders_calls 
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
	updated_at,
	now('Europe/Samara') AS update_etl
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loader_calls',
	'airflow_etl',
	'airpegas',
	'public')
where
	close_time is not Null and
	toUnixTimestamp(updated_at) > (
	select
		coalesce(max(toUnixTimestamp(updated_at)), 0)
	from
		bronze_layer.loaders_calls);




--RENAME TABLE bronze_layer.loaders_calls TO bronze_layer.loaders_calls_old;
--
--RENAME TABLE bronze_layer.loaders_calls_new TO bronze_layer.loaders_calls;

