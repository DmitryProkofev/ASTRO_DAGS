#

select
	lc.id, 
	dc.DateKey as datetime_key_open,
	cmg.TimeKey as time_key_open,
	dpo.id_srgt as customer_id,
	dlr.srgt_id as reason_id,
	dlw.srgt_id as workshop_id,
	dpo2.id_srgt as loader_id
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
; 



DESCRIBE TABLE silver_layer.pa_oper;


#