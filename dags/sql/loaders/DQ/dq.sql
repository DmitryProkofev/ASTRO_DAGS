

--- запрос проверки минусовых вычислений
select
	id_oltp,
	dif_day_open_taken,
	dif_time_open_taken,
	dif_day_taken_close,
	dif_time_taken_close,
	dif_day_open_close,
	dif_time_open_close
from
	cdm.loaders_dif_time_gen
where
	dif_day_open_taken < 0
	or dif_time_open_taken < 0
	or
dif_day_taken_close < 0
	or dif_time_taken_close < 0
	or dif_day_open_close < 0
	or dif_time_open_close < 0;




------------------ DQ на таблицу фактов --------------------------

-- 🔑 `foreign key` — на измерения (связь с dimension-таблицами)
--- по идее делать проверку перед записью в таблицу фактов, т.е. из silver_layer в test_db
-- также здесь проверка дат на не вхождние в таблицу измерений дат


INSERT INTO
	quality.etl_data_quality_log (
	process_name,
	table_name,
	column_name,
	record_key,
	error_type,
	error_message)
select
	'loaders',
	'test_db.fct_loaders_calls',
	col_name,
	id_oltp,
	'data consistency',
	'NOT data consistency in fact table'
from (
select lc.id as id_oltp, 'datetime_key_open' as col_name
from silver_layer.loaders_calls lc
left join test_db.dim_calendar dc ON
	dc.`Date` = toDate(formatDateTime(lc.open_time, '%Y-%m-%d'))
where dc.`DateKey` = 0
UNION ALL
SELECT lc.id as id_oltp, 'customer_id' as col_name
FROM silver_layer.loaders_calls lc
LEFT JOIN test_db.dim_pa_oper_actual dpo ON lc.customer_id = dpo.tg_id
WHERE dpo.id_srgt = 0
UNION ALL
SELECT lc.id as id_oltp, 'reason_id' as col_name
FROM silver_layer.loaders_calls lc
LEFT JOIN test_db.dim_loaders_reasons_actual dlr ON lc.call_reason_id = dlr.id
WHERE dlr.id = 0
UNION ALL
SELECT lc.id as id_oltp, 'workshop_id' as col_name
FROM silver_layer.loaders_calls lc
LEFT JOIN test_db.dim_loaders_workshops_actual dlr ON lc.workshop_id = dlr.id
where dlr.id = 0
UNION ALL
SELECT lc.id as id_oltp, 'loader_id' as col_name
FROM silver_layer.loaders_calls lc
LEFT JOIN test_db.dim_pa_oper_actual dlr ON dlr.tg_id = lc.loader_id
where dlr.tg_id = 0
UNION ALL
select lc.id as id_oltp, 'datetime_key_taken' as col_name
from silver_layer.loaders_calls lc
left join test_db.dim_calendar cmg ON
	cmg.Date = toDate(formatDateTime(lc.taken_time, '%Y-%m-%d'))
where cmg.DateKey = 0
UNION ALL
select lc.id as id_oltp, 'datetime_key_close' as col_name
from silver_layer.loaders_calls lc
left join test_db.dim_calendar cmg ON
	cmg.Date = toDate(formatDateTime(lc.close_time, '%Y-%m-%d'))
where cmg.DateKey = 0
UNION ALL
select lc.id as id_oltp, 'priority_id' as col_name
from silver_layer.loaders_calls lc
left join test_db.dim_loaders_call_priorities_actual cmg ON lc.priority = cmg.id
where cmg.id = 0);


-- ❌ отрицательные значения в числах (например, `amount`, `quantity`)
INSERT INTO
	quality.etl_data_quality_log (
	process_name,
	table_name,
	column_name,
	record_key,
	error_type,
	error_message)
select
	'loaders',
	'public.dq_test',
	'container_qty',
	id_oltp,
	'not valid value',
	'negative values'
from
	(
	select id_oltp from default.dq_test flc 
where container_qty < 0) dup
join default.dq_test flc ON
	flc.id_oltp = dup.id_oltp;


    
-- 💥 дубликаты по бизнес-ключам (`order_id + date`)

INSERT INTO
	quality.etl_data_quality_log (
	process_name,
	table_name,
	column_name,
	record_key,
	error_type,
	error_message)
select
	'loaders',
	'public.dq_test',
	'id_oltp',
	id_oltp,
	'duplicate_key',
	'duplicate id_oltp'
from
	(
	select
		id_oltp
	from
		test_db.fct_loaders_calls flc
	group by
		flc.id_oltp
	having
		count() > 1) dup
join test_db.fct_loaders_calls flc ON
	flc.id_oltp = dup.id_oltp;






----------------------------- DQ на таблицы измерений --------------------------

-- 🔑 уникальность `primary key` (`customer_id`, `product_code`)


    
-- 🧩 непротиворечивость атрибутов (например, `country_code` в списке допустимых значений)



-- 📆 контроль версий (`valid_from`, `valid_to`) если используешь SCD



-- 🎭 формат значений (например, email, телефон, ИНН)
    

