

INSERT INTO gold_layer.fct_loaders_calls
select
	lc.id, 
	dc.DateKey as datetime_key_open,
	cmg.TimeKey as time_key_open,
	dpo.id_srgt as customer_id,C:\Users\d.prokofev\VSCodeProjects\AIRFOW_ASTRO\dags\sql\loaders\silver\loaders_calls_silver.sql
	dlr.srgt_id as reason_id,
	dlw.srgt_id as workshop_id,
	dpo2.id_srgt as loader_id,
	c2.DateKey as datetime_key_taken,
	cmg2.TimeKey as time_key_taken,
	c3.DateKey as datetime_key_close,
	cmg3.TimeKey as time_key_close,
	dlcp.srgt_id as prority_id,
	lc.container_qty as container_qty,
	lc.updated_at as updated_at,
	now('Europe/Samara') AS update_etl
from
	silver_layer.loaders_calls lc
left join gold_layer.dim_calendar dc ON
	dc.`Date` = toDate(formatDateTime(lc.open_time, '%Y-%m-%d'))
left join gold_layer.dim_calendar_minute_grain cmg ON
	cmg.`Time` = formatDateTime(lc.open_time, '%H:%i') 
left join gold_layer.dim_pa_oper dpo ON
lc.customer_id = dpo.tg_id
left join gold_layer.dim_loaders_reasons dlr ON
lc.call_reason_id = dlr.id
left join gold_layer.dim_loaders_workshops dlw ON
lc.workshop_id = dlw.id
left join gold_layer.dim_pa_oper dpo2 ON
lc.loader_id = dpo2.tg_id
left join gold_layer.dim_calendar c2 ON
	c2.Date = toDate(formatDateTime(lc.taken_time, '%Y-%m-%d'))
left join gold_layer.dim_calendar_minute_grain cmg2 ON
	cmg2.`Time` = formatDateTime(lc.taken_time, '%H:%i')
left join gold_layer.dim_calendar c3 ON
	c3.`Date` = toDate(formatDateTime(lc.close_time, '%Y-%m-%d'))
left join gold_layer.dim_calendar_minute_grain cmg3 ON
	cmg3.`Time` = formatDateTime(lc.close_time, '%H:%i')
left join gold_layer.dim_loaders_call_priorities dlcp ON
lc.priority = dlcp.id;
