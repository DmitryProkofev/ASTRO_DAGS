#TODO используем инкрементальную загрузку

--- Таблица погрузчиков в первый базовый staging слой clickhouse без обработки, НО все же с выборкой определенных полей
create table staging.loaders_calls
(	
    id UInt32,
    open_time DateTime,
    customer_id UInt64,
    workshop_id UInt8,
    call_reason_id UInt8,
    comment String,
    loader_id UInt64,
    taken_time DateTime,
    close_time DateTime,
    priority UInt8,
    container_qty UInt8,
    update_data DateTime
) 
ENGINE = MergeTree()
ORDER BY id;


drop table staging.loaders_calls;

--- здесь забираем данные по последнему id
create table bronze_layer.loaders_calls engine=MergeTree order by id as 
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
where close_time is not Null;

#TODO добавить  and (select max(id) from staging.loaders_calls);



DROP TABLE bronze_layer.loaders_calls;