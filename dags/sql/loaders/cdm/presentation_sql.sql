
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
	cdm.loaders_dif_time_gen where 
dif_day_open_taken < 0
or dif_time_open_taken < 0 or
dif_day_taken_close < 0 or dif_time_taken_close < 0 or dif_day_open_close < 0 or dif_time_open_close < 0;



select
	dpo.operator,
	dc.`Year`,
	dc.`Month`,
	median(lg.dif_time_open_taken) AS median_open_taken,
	topK(1)(lg.dif_time_open_taken)[1] AS mode_open_taken,
	median(lg.dif_time_taken_close) AS median_taken_close,
	topK(1)(lg.dif_time_taken_close)[1] AS mode_taken_close,
	median(lg.dif_time_open_close) AS median_open_close,
	topK(1)(lg.dif_time_open_close)[1] AS mode_open_close
from
	cdm.loaders_dif_time_gen lg
left join gold_layer.dim_calendar dc 
ON
	dc.DateKey = lg.datetime_key_open
left join gold_layer.dim_pa_oper dpo
ON
	dpo.id_srgt = lg.loader_id
group by
	dpo.operator,
	dc.`Year`,
	dc.`Month`;


    