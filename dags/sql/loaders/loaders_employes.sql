insert into dim_layer.dim_loaders_employes
SELECT
	tg_id,
	operator,
	is_loader
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'pa_oper',
	'airflow_etl',
	'airpegas',
	'public')
where
	tg_id is not null OR tg_id NOT IN (5773698501, 325813539);