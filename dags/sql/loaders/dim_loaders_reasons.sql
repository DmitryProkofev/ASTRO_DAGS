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