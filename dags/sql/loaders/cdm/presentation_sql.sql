create view cdm.loaders_dif_time_taken_close_view
as
SELECT
	id_oltp,
	updated_at,
	update_etl,
	sum(multiIf((cmg.Rest = 0) AND (cmg.Change = 1),
 1,
 0)) AS count_minute
FROM
	gold_layer.fct_loaders_calls AS flc
INNER JOIN gold_layer.dim_calendar AS c ON
	((c.DateKey >= flc.datetime_key_taken)
		AND (c.DateKey <= flc.datetime_key_close))
	AND (c.DayOfWeek NOT IN (6,
 7))
INNER JOIN gold_layer.dim_calendar_minute_grain AS cmg ON
	(cmg.TimeKey >= flc.time_key_taken)
	AND (cmg.TimeKey <= flc.time_key_close)
GROUP BY
	id_oltp,
	updated_at,
	update_etl
COMMENT 'Представление для анализа расчета времени в минутах между временем взятия и закрытия вызова.\r\n Считает активные минуты (Rest=0 и Change=1) для вызовов,
которые длились менее 1 рабочего дня (исключая выходные)';



---------------------------------------


CREATE VIEW cdm.loaders_dif_time_open_taken_view as
SELECT
    id_oltp,
    updated_at,
    update_etl,
    sum(multiIf((cmg.Rest = 0) AND (cmg.Change = 1), 1, 0)) AS count_minute
FROM gold_layer.fct_loaders_calls AS flc
INNER JOIN gold_layer.dim_calendar AS c 
    ON (c.DateKey >= flc.datetime_key_open) 
    AND (c.DateKey <= flc.datetime_key_taken) 
    AND (c.DayOfWeek NOT IN (6, 7))
INNER JOIN gold_layer.dim_calendar_minute_grain AS cmg 
    ON (cmg.TimeKey >= flc.time_key_open) 
    AND (cmg.TimeKey <= flc.time_key_taken)
GROUP BY
    id_oltp,
    updated_at,
    update_etl
COMMENT 'Представление для анализа расчета времени в минутах между временем создания и взятия вызова.\r\n Считает активные минуты (Rest=0 и Change=1) для вызовов,
 которые длились менее 1 рабочего дня (исключая выходные)';
    

    
