#TODO  добавить расчеты для второй смены


CREATE DATABASE bronze_layer;
CREATE DATABASE silver_layer;
CREATE DATABASE gold_layer;


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




and (select max(id) from staging.loaders_calls);

DROP TABLE bronze_layer.loaders_calls;
	
	
	-- вывести в обработку на другом слое. в серебряный слой
	WHERE customer_id NOT IN (5773698501, 325813539)
  AND loader_id NOT IN (5773698501, 325813539)
and close_time is not Null
and id > (select max(id) from staging.loaders_calls);


--- базовый DQ 
select id, count(*) from staging.loaders_calls lc 
group by id HAVING count(*) > 1;




---------------------------- таблица с информацией погрузчитков/заказчиков  ----------------------------------------
#TODO тут ошибка где-то в этом запросе

CREATE TABLE staging.dim_loaders_employes
(
    id UInt64,
    operstor String,
    is_loader UInt8,
    update_data DateTime
) 
ENGINE = ReplacingMergeTree(update_data)
ORDER BY id;


insert into staging.dim_loaders_employes
SELECT
	tg_id,
	operator,
	is_loader,
	now('Europe/Samara') AS update_data
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'pa_oper',
	'airflow_etl',
	'airpegas',
	'public')
where
	tg_id is not null OR tg_id NOT IN (5773698501, 325813539);



--  DQ

select id, count(*) from dim_layer.dim_loaders_employes lc 
group by id HAVING count(*) > 1;



---------------------------- таблица Перечень местоположений для вызова погрузчиков  --------------------------------------

CREATE TABLE dim_layer.dim_loaders_workshops
(
    id UInt32,
    worlshop_name String,
    worlshop_description String,
    update_data DateTime
) 
ENGINE = ReplacingMergeTree(update_data)
ORDER BY id;


insert
	into
	dim_layer.dim_loaders_workshops
select
	*,
	now('Europe/Samara') AS update_data
from
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_workshops',
	'airflow_etl',
	'airpegas',
	'public');




--------------------------------------------------- Таблица причин для вызовов погрузчиков -------------------------------------
CREATE TABLE dim_layer.dim_loaders_reasons
(
    id UInt32,
    reason String,
    update_data DateTime
) 
ENGINE = ReplacingMergeTree(update_data)
ORDER BY id;



insert
	into
	dim_layer.dim_loaders_reasons
select
	id,
	reason,
	now('Europe/Samara') AS update_data
from
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_reasons',
	'airflow_etl',
	'airpegas',
	'public');


----------------------------------------------------------Таблица дат ---------------------------------------------------

--- Сюда добавить таблицу с временем по минутам втечение дня и к ней добавить:
-- маркер выходного дня
-- маркер рабочего времени смены
--- маркер перерывов


CREATE TABLE IF NOT EXISTS dim_layer.calendar (
		DateKey UInt32,
        Date Date, --- тут дата без времени
        Year Int32,
        Month Int32,
        DayOfMonth Int32,
        DayOfWeek Int32,
        WeekOfYear Int32,
        IsWeekend Int32,
        IsHoliday Int32
    ) 
ENGINE = MergeTree
ORDER BY DateKey;

-- данные для календаря добавляются через python

-- создание таблицы изменений врнемени с зернистостью по минутам

CREATE TABLE IF NOT EXISTS dim_layer.calendar_minute_grain (
	TimeKey UInt32,
    Time String,         -- Время
    Hour Int32,            -- Час (0-23)
    Minute Int32,          -- Минута (0-59)
    Change Int8,         --- Смена
    Rest Int8           --- Флаг рабочего времени. 1- не рабочее время
)
ENGINE = MergeTree
ORDER BY TimeKey;



insert into dim_layer.calendar_minute_grain 
SELECT
	TimeKey,
	Time,
	Hour,
	Minute,
	Change,
	Rest
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'calendar_minute_grain',
	'compaint_bot_role',
	'123123pegas',
	'airflow_data')
;



---------------   ТАБЛИЦА ПРИОРИТЕТОВ ---------------------------------

CREATE TABLE IF NOT EXISTS dim_layer.dim_priority (
	id UInt8,
    priority_name String,
    priority_desc String,
    update_data DateTime
)
ENGINE = ReplacingMergeTree(update_data)
ORDER BY (id);


insert
	into
	dim_layer.dim_priority
select
	id,
	priority_name,
	priority_desc,
	now('Europe/Samara') AS update_data
from
	postgresql('10.1.11.17:5432',
	'AGRO',
	'loaders_call_priorities',
	'airflow_etl',
	'airpegas',
	'public');

select * from dim_layer.dim_priority
where max(update_data);





--------------------------------------------------------- TАБЛИЦА ФАКТОВ ----------------------------------------

#TODO добавить FINAL
#TODO тут нужен DQ обязательно



#TODO тут будут вставляться только новые записи
--insert into stg_facts.fact_loader_calls
create table stg_facts.fact_loader_calls engine=MergeTree order by call_id as 
SELECT
	lc.id as call_id, 
	c.DateKey as datetime_key_open, 		 -- Ключ таблицы измерений даты (открытие вызова)
	cmg.TimeKey as time_key_open,	 		 -- Ключ таблицы измерений времени (открытие вызова)
	lc.customer_id as employee_customer_id,  -- ID заказчиков
	lc.loader_id  as employee_loader_id,    -- ID погрзчиков
	dlr.id as reason_id, 		 			-- ID причины вызова
	dlw.id as workshop_id, 		            -- ID цеха
	c2.DateKey as datetime_key_taken,      -- Ключ таблицы измерений даты (взятие вызова)
	cmg2.TimeKey as time_key_taken,	       -- Ключ таблицы измерений времени (взятие вызова)
	c3.DateKey as datetime_key_close,		 -- Ключ таблицы измерений даты (закрытие вызова)
	cmg3.TimeKey as time_key_close,	 		-- Ключ таблицы измерений времени (закрытие вызова)
	dp.id as priority_id,
	lc.container_qty as container_qty,
	now('Europe/Samara') AS update_data
FROM
	staging.loaders_calls FINAL lc
left join dim_layer.calendar c ON
	c.`Date` = toDate(formatDateTime(lc.open_time, '%Y-%m-%d'))
left join dim_layer.calendar_minute_grain cmg ON
	cmg.`Time` = formatDateTime(lc.open_time, '%H:%i')
left join dim_layer.dim_loaders_employes FINAL dle ON
	dle.id = lc.customer_id
left join dim_layer.dim_loaders_reasons FINAL dlr ON
	dlr.id = lc.call_reason_id
left join dim_layer.dim_loaders_workshops FINAL dlw ON
	dlw.id = lc.workshop_id
left join dim_layer.calendar FINAL c2 ON
	c2.`Date` = toDate(formatDateTime(lc.taken_time, '%Y-%m-%d'))
left join dim_layer.calendar_minute_grain cmg2 ON
	cmg2.`Time` = formatDateTime(lc.taken_time, '%H:%i')
left join dim_layer.calendar c3 ON
	c3.`Date` = toDate(formatDateTime(lc.close_time, '%Y-%m-%d'))
left join dim_layer.calendar_minute_grain cmg3 ON
	cmg3.`Time` = formatDateTime(lc.close_time, '%H:%i')
left join dim_layer.dim_priority FINAL dp ON
	dp.id = lc.priority
;


--- запрос с количеством времени в разрезе дня
select
	flc.call_id, 
	flc.datetime_key_taken,
	flc.datetime_key_close, 
	count(c.DateKey) - 1 as count_day
from
	stg_facts.fact_loader_calls flc
	join dim_layer.calendar c ON c.DateKey BETWEEN flc.datetime_key_taken AND flc.datetime_key_close
	AND c.DayOfWeek NOT IN (6, 7) -- убираем субботу и воскресенье
group by
flc.call_id,
	flc.datetime_key_taken,
	flc.datetime_key_close;


--- запрос с количеством времени в разрезе минут для первой смены

select
	flc.call_id, 
	flc.time_key_taken,
	flc.time_key_close, 
	count(cmg.TimeKey) as count_minute
from
	stg_facts.fact_loader_calls flc
	join dim_layer.calendar_minute_grain cmg  ON cmg.TimeKey BETWEEN flc.time_key_taken  AND flc.time_key_close 
	AND cmg.Rest = 0 AND Change = 1
group by
flc.call_id, 
	flc.time_key_taken,
	flc.time_key_close;



--- основное вычисление количества минут для ПЕРВОЙ смены на каждый вызов. Делаем вьюшку

#TODO здесь добавить расчет  времени реагирования между созданным вызовом и принятым погрузчиком

CREATE VIEW calc.dif_time_loaders AS
SELECT 
    flc.call_id as call_id, 
 --   flc.employee_loader_id,
    count_minute -- количество времени вызова
FROM
    stg_facts.fact_loader_calls flc
JOIN (
    -- Подзапрос для расчета количества дней (не включая выходные)
    SELECT
        flc.call_id, 
        flc.datetime_key_taken,
        flc.datetime_key_close, 
        count(c.DateKey) - 1 as count_day
    FROM
        stg_facts.fact_loader_calls flc
    JOIN dim_layer.calendar c 
        ON c.DateKey BETWEEN flc.datetime_key_taken AND flc.datetime_key_close
        AND c.DayOfWeek NOT IN (6, 7) -- исключаем субботу и воскресенье
    GROUP BY
        flc.call_id,
     --   flc.employee_loader_id,
        flc.datetime_key_taken,
        flc.datetime_key_close
) AS day_agg
    ON flc.call_id = day_agg.call_id
    AND flc.datetime_key_taken = day_agg.datetime_key_taken
    AND flc.datetime_key_close = day_agg.datetime_key_close
JOIN (
    -- Подзапрос для расчета количества минут
    SELECT
        flc.call_id, 
        flc.time_key_taken,
        flc.time_key_close, 
        count(cmg.TimeKey) as count_minute
    FROM
        stg_facts.fact_loader_calls flc
    JOIN dim_layer.calendar_minute_grain cmg  
        ON cmg.TimeKey BETWEEN flc.time_key_taken AND flc.time_key_close
        AND cmg.Rest = 0 
        AND Change = 1 -- смена 1
    GROUP BY
        flc.call_id, 
        flc.time_key_taken,
        flc.time_key_close
) AS minute_agg
    ON flc.call_id = minute_agg.call_id
    AND flc.time_key_taken = minute_agg.time_key_taken
    AND flc.time_key_close = minute_agg.time_key_close
where count_day < 1; -- отфильтруем заказы, которые выполнялись больше одного дня;
#TODO фильтровать заказы, которые выполнялись более часа



--- создаем таблицу фактов с предрассчитанными данными по времени выполнения вызова



CREATE TABLE calc.fact_loader_calls_count_minute_one_change
(
    call_id UInt32,                         -- Уникальный идентификатор вызова
    datetime_key_open UInt32,                  -- Внешний ключ на calendar
    time_key_open UInt32,
    employee_loader_id UInt32,            -- Внешний ключ на dim_loaders_employes (погрузчик)
    employee_customer_id UInt32,          -- Внешний ключ на dim_loaders_employes (заказчик)
    reason_id UInt32,                     -- Внешний ключ на dim_loaders_reasons
    workshop_id UInt32,                   -- Внешний ключ на dim_loaders_workshops
    datetime_key_taken UInt32,            -- Внешний ключ на calendar
    time_key_taken UInt32,
    datetime_key_close UInt32,            -- Внешний ключ на calendar
    time_key_close UInt32,
    is_loader UInt8,                      -- Флаг, был ли это погрузчик (дублируем для быстрого анализа)
    priority_id UInt8,					  -- ID приоритета вызова
    count_minute UInt8-- количество времени потраченное на вызов
)
ENGINE = MergeTree()
ORDER BY (call_id);



create table calc.fact_loader_calls_count_minute_one_change engine = MergeTree order by call_id as
select
	flc.call_id as call_id,
	-- Уникальный идентификатор вызова
	flc.datetime_key_open as datetime_key_open,
	-- Внешний ключ на calendar
	flc.time_key_open as time_key_open,
	flc.employee_customer_id as employee_customer_id,
	-- Внешний ключ на dim_loaders_employes (заказчик)
	flc.employee_loader_id as employee_loader_id,
	-- Внешний ключ на dim_loaders_employes (погрузчик)
	flc.reason_id as reason_id,
	-- Внешний ключ на dim_loaders_reasons
	flc.workshop_id as workshop_id,
	-- Внешний ключ на dim_loaders_workshops
	flc.datetime_key_taken as datetime_key_taken,
	-- Внешний ключ на calendar
	flc.time_key_taken as time_key_taken,
	flc.datetime_key_close as datetime_key_close,
	-- Внешний ключ на calendar
	flc.time_key_close as time_key_close,
	flc.priority_id as priority_id,
	-- ID приоритета вызова
	flc.container_qty as container_qty,
	dtl.count_minute as count_minute
from
	calc.dif_time_loaders as dtl
left join stg_facts.fact_loader_calls flc ON
	flc.call_id = dtl.call_id;




------------------------------
--- сформируем первую витрину в разрезе месяца по погрузчикам
#TODO отфильтровать не работающих погрузчиков

create table cdm.loader_timer_mounth engine=MergeTree order by period_date as 
SELECT
    toDate(concat(toString(c."Year"), '-', lpad(toString(c."Month"), 2, '0'), '-01')) AS period_date,
    dle.id as id,
    dle.operstor as loader,
    avg(flccmoc.count_minute) AS avg_count_minute, 
    min(flccmoc.count_minute) AS min_count_minute,
    max(flccmoc.count_minute) AS max_count_minute
FROM
    calc.fact_loader_calls_count_minute_one_change flccmoc
left JOIN dim_layer.calendar c 
    ON c.DateKey = flccmoc.datetime_key_open 
left JOIN dim_layer.dim_loaders_employes dle 
    ON dle.id = flccmoc.employee_loader_id
GROUP BY
    period_date,
    id,
    loader
    
ORDER BY
    period_date;




select * from dim_layer.dim_loaders_employes dle 
WHERE dle.is_loader = 1;


select employee_loader_id from calc.fact_loader_calls_count_minute_one_change flccmoc 
group by flccmoc.employee_loader_id;



--- запрос в разрезе даты сгруппированной по месяцам. 

--insert into cdm.loader_timer

CREATE TABLE cdm.loader_timer
ENGINE = ReplacingMergeTree(update_data)  -- Столбец, по которому будет определяться замена
ORDER BY period_date
AS
--insert into cdm.loader_timer
SELECT
    toDate(concat(toString(c."Year"), '-', lpad(toString(c."Month"), 2, '0'), '-01')) AS period_date,
    avg(flccmoc.count_minute) AS avg_count_minute, 
    min(flccmoc.count_minute) AS min_count_minute,
    max(flccmoc.count_minute) AS max_count_minute,
    now('Europe/Samara') AS update_data
FROM
    calc.fact_loader_calls_count_minute_one_change flccmoc
JOIN dim_layer.calendar c 
    ON c.DateKey = flccmoc.datetime_key_open 
GROUP BY
    period_date
ORDER BY
    period_date;


OPTIMIZE TABLE cdm.loader_timer FINAL; -- удаляем дубли




--- по годам
--create table cdm.loader_timer_year engine=MergeTree order by period_date as
SELECT
    toDate(c."Year") AS period_date,
    avg(flccmoc.count_minute) AS avg_count_minute, 
    min(flccmoc.count_minute) AS min_count_minute,
    max(flccmoc.count_minute) AS max_count_minute
FROM
    calc.fact_loader_calls_count_minute_one_change flccmoc
JOIN dim_layer.calendar c 
    ON c.DateKey = flccmoc.datetime_key_open 
GROUP BY
    period_date
ORDER BY
    period_date;

select * from cdm.loader_timer_year;


-- среднее врея вызова по году/месяцу в одной таблице
--create table cdm.loader_timer_year_and_mounth engine=MergeTree order by period_date as
SELECT
    toDate(concat(toString(c."Year"), '-', lpad(toString(c."Month"), 2, '0'), '-01')) AS period_date,
    'month' AS grain,
    avg(flccmoc.count_minute) AS avg_count_minute
FROM calc.fact_loader_calls_count_minute_one_change flccmoc
JOIN dim_layer.calendar c ON c.DateKey = flccmoc.datetime_key_open
GROUP BY period_date

UNION ALL

SELECT
    toDate(concat(toString(c."Year"), '-01-01')) AS period_date,
    'year' AS grain,
    avg(flccmoc.count_minute) AS avg_count_minute
FROM calc.fact_loader_calls_count_minute_one_change flccmoc
JOIN dim_layer.calendar c ON c.DateKey = flccmoc.datetime_key_open
GROUP BY c."Year";






SELECT
  update_data::date AS update_date,
  COUNT(*) AS updates_count,
  RANK() OVER (ORDER BY COUNT(*) DESC) AS date_rank
FROM
  staging.loaders_calls
GROUP BY
  update_date;


SELECT
  update_data,
  RANK() OVER (ORDER BY update_data DESC) AS update_rank
FROM (
  SELECT DISTINCT update_data
  FROM staging.loaders_calls
) t;



SELECT id, MAX(update_data)
  FROM staging.loaders_calls
  GROUP BY id;




TRUNCATE TABLE bronze_layer.pa_oper;
TRUNCATE TABLE bronze_layer.loaders_call_priorities;
TRUNCATE TABLE bronze_layer.loaders_calls;
TRUNCATE TABLE bronze_layer.loaders_reasons;
TRUNCATE TABLE bronze_layer.loaders_workshops;



DROP TABLE IF EXISTS bronze_layer.pa_oper_old;
