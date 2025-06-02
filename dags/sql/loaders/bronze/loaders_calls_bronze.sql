#TODO используем инкрементальную загрузку


--- здесь забираем данные по последнему id
--create table bronze_layer.loaders_calls engine=MergeTree order by id as
insert into bronze_layer.loaders_calls 
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
	now('Europe/Samara') AS update_data
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loader_calls',
	'airflow_etl',
	'airpegas',
	'public')
where close_time is not Null
and id > (select max(id) from silver_layer.loaders_calls);


--TRUNCATE TABLE bronze_layer.loaders_calls;