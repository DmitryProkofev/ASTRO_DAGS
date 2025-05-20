# Отчет(вьюшка в оракл) LOTS_PRINT, которая участвует в BI_аналитике
-- Таблицы из оракла отображаются через внешнее подключение в postgres
-- Все таки возможно нужен еще staging слой на уровне postgresql. Там будут обработки типа case/when, какие то простенькие предобработки. никаких аналитических расчетов
-- пока обойтись без olap куба. просто переложить в clickhouse и сравнить скорость и нагрузку

--- LOTS_OPERATIONS2 и LOTS_OPERATIONS2_NEW потенциальные витрины
-- по сути любая вьюшка как потенциальная витрина. во вьюшках еще есть запросы с агрегациями смахивающими как раз на витрины

--- в таблицах, которые участвуют в построение дашборда используется всего лишь несколько столбцов
## TODO брать тольку НУЖНЫЕ для запроса!!! но пока берем все

## TODO нужен MinIO  для стеджинга крупных таблиц

#TODO продумать обновление (самый очевидный/тупой способ по последним id таблицы), чтобы не забирать из OLTP все записи. Триггер и все такое

create table staging.slocation_agro engine=MergeTree order by sk_id as 
SELECT
--row_number() OVER () AS sk_id, --- сурогатный ключ
now('Europe/Samara') AS update_data,
*
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'slocation_agro',
	'compaint_bot_role',
	'123123pegas',
	'stage');


create table staging.pa_oper engine=MergeTree order by sk_id as 
SELECT
now('Europe/Samara') AS update_data,
*
FROM
	postgresql('10.1.11.17:5432',
	'AGRO',
	'pa_oper',
	'compaint_bot_role',
	'123123pegas',
	'stage');



#TODO продумать обновление (самый очевидный/тупой способ по последним id таблицы)
create table staging.pa_lotswp engine=ReplacingMergeTree(update_data) order by idlotswpjob as 
INSERT INTO staging.pa_lotswp
SELECT *,
now('Europe/Samara') AS update_data
FROM postgresql(
  '10.1.11.17:5432',
  'AGRO',
  'pa_lotswp',
  'compaint_bot_role',
  '123123pegas',
  'stage'
)
WHERE idlotswpjob between  3 and 1000000;




--- получить финальную версию таблицы (без дублей последнюю версию)
select * from staging.pa_lotswp final;



