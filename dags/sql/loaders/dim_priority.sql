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