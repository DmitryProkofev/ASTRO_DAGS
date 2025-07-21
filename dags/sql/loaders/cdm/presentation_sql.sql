
create view cdm.loaders_dif_time_gen
as
select
	*,
	(flc.datetime_key_taken - flc.datetime_key_open) as dif_day_open_taken,
	(flc.time_key_taken - flc.time_key_open) as dif_time_open_taken,
	(flc.datetime_key_close - flc.datetime_key_taken) as dif_day_taken_close,
	(flc.time_key_close - flc.time_key_taken) as dif_time_taken_close,
	(flc.datetime_key_close - flc.datetime_key_open) as dif_day_open_close,
	(flc.time_key_close - flc.time_key_open) as dif_time_open_close
from
	gold_layer.fct_loaders_calls flc
where
	dif_day_open_taken = 0
	and
dif_day_taken_close = 0
	and
dif_day_open_close = 0 and 
dif_time_taken_close <> 0 and 
dif_time_open_close <> 0
COMMENT 'Представление для анализа расчета времени разницы по во времени по меткам:
dif_day_open_taken - тут описание поля.
С фильтрами где разница в количестве дней равна нулю, а количество минут не равно нулю (погрузчик не работал в этот день)';


--- витрина, показывающая моду и медиану по времени работы погрузчиков
-- в разрезе месяца
CREATE TABLE cdm.loaders_mounth_time
ENGINE = MergeTree
ORDER BY (operator, ifNull(year_month, '0000-00'))
AS
SELECT
    dpo.operator,
    dc.`Year`,
    dc.`Month`,
    formatDateTime(toDate(concat(toString(dc.`Year`), '-', toString(dc.`Month`), '-01')), '%Y-%m') AS year_month,
    median(lg.dif_time_open_taken) AS median_open_taken,
    topK(1)(lg.dif_time_open_taken)[1] AS mode_open_taken,
    median(lg.dif_time_taken_close) AS median_taken_close,
    topK(1)(lg.dif_time_taken_close)[1] AS mode_taken_close,
    median(lg.dif_time_open_close) AS median_open_close,
    topK(1)(lg.dif_time_open_close)[1] AS mode_open_close
FROM cdm.loaders_dif_time_gen lg
LEFT JOIN gold_layer.dim_calendar dc 
    ON dc.DateKey = lg.datetime_key_open
LEFT JOIN gold_layer.dim_pa_oper_actual dpo
    ON dpo.id_srgt = lg.loader_id
GROUP BY
    dpo.operator,
    dc.`Year`,
    dc.`Month`
COMMENT 'Витрина, показывающая моду и медиану по времени работы погрузчиков в разрезе месяца'


--- витрина, показывающая моду и медиану по времени работы погрузчиков
-- в разрезе дня
CREATE TABLE cdm.loaders_day
ENGINE = MergeTree
ORDER BY (full_date, operator)
AS
SELECT
    dpo.operator,
    dc.`Year`,
    dc.`Month`,
    dc.DayOfMonth,
    formatDateTime(toDate(concat(toString(dc.`Year`), '-', toString(dc.`Month`), '-', toString(dc.DayOfMonth))), '%Y-%m-%d') AS full_date,
    median(lg.dif_time_open_taken) AS median_open_taken,
    topK(1)(lg.dif_time_open_taken)[1] AS mode_open_taken,
    median(lg.dif_time_taken_close) AS median_taken_close,
    topK(1)(lg.dif_time_taken_close)[1] AS mode_taken_close,
    median(lg.dif_time_open_close) AS median_open_close,
    topK(1)(lg.dif_time_open_close)[1] AS mode_open_close
FROM cdm.loaders_dif_time_gen lg
LEFT JOIN gold_layer.dim_calendar dc 
    ON dc.DateKey = lg.datetime_key_open
LEFT JOIN gold_layer.dim_pa_oper_actual dpo
    ON dpo.id_srgt = lg.loader_id
GROUP BY
    dpo.operator,
    dc.`Year`,
    dc.`Month`,
    dc.DayOfMonth,
    full_date;

---TODO сделать в разрезе складов
CREATE TABLE cdm.loaders_mounth_time_sklad
ENGINE = MergeTree
ORDER BY (workshop_name, ifNull(year_month, '0000-00'))
AS
SELECT
    dpo.workshop_name,
    dc.`Year`,
    dc.`Month`,
    formatDateTime(toDate(concat(toString(dc.`Year`), '-', toString(dc.`Month`), '-01')), '%Y-%m') AS year_month,
    median(lg.dif_time_open_taken) AS median_open_taken,
    topK(1)(lg.dif_time_open_taken)[1] AS mode_open_taken,
    median(lg.dif_time_taken_close) AS median_taken_close,
    topK(1)(lg.dif_time_taken_close)[1] AS mode_taken_close,
    median(lg.dif_time_open_close) AS median_open_close,
    topK(1)(lg.dif_time_open_close)[1] AS mode_open_close
FROM cdm.loaders_dif_time_gen lg
LEFT JOIN gold_layer.dim_calendar dc 
    ON dc.DateKey = lg.datetime_key_open
LEFT JOIN gold_layer.dim_loaders_workshops_actual dpo
    ON dpo.srgt_id = lg.workshop_id
GROUP BY
    dpo.workshop_name,
    dc.`Year`,
    dc.`Month`;




SELECT
    dpo.operator,
    dc.`Year`,
    dc.`Month`,
    formatDateTime(toDate(concat(toString(dc.`Year`), '-', toString(dc.`Month`), '-01')), '%Y-%m') AS year_month,
    median(lg.dif_time_open_taken) AS median_open_taken,
    topK(1)(lg.dif_time_open_taken)[1] AS mode_open_taken,
    median(lg.dif_time_taken_close) AS median_taken_close,
    topK(1)(lg.dif_time_taken_close)[1] AS mode_taken_close,
    median(lg.dif_time_open_close) AS median_open_close,
    topK(1)(lg.dif_time_open_close)[1] AS mode_open_close
FROM cdm.loaders_dif_time_gen lg
LEFT JOIN gold_layer.dim_calendar dc 
    ON dc.DateKey = lg.datetime_key_open
LEFT JOIN gold_layer.dim_pa_oper_actual dpo
    ON dpo.id_srgt = lg.loader_id
GROUP BY
    dpo.operator,
    dc.`Year`,
    dc.`Month`;



