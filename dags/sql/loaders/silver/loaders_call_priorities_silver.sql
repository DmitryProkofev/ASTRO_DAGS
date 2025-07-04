--- TODO на этом сллое происходит дедубликация через оконную функцию
-- по идее можно было бы пропустить этот слой и через оконку инсертить сразу в таблицу измерений,
--- но подразумевается что тут будет какая то обработка.


CREATE TABLE silver_layer.loaders_call_priorities_new engine = MergeTree
ORDER BY
id AS
select
	id,
	priority_name,
	priority_desc,
	emojie,
	updated_at,
	now('Europe/Samara') AS update_etl
from
	(
	SELECT
		*,
		ROW_NUMBER() OVER (
            PARTITION BY id
	ORDER BY
		update_etl DESC
        ) AS rn
	FROM
		bronze_layer.loaders_call_priorities) sub
where
	rn = 1;



--RENAME TABLE silver_layer.loaders_call_priorities TO silver_layer.loaders_call_priorities_old;
--
--
--RENAME TABLE silver_layer.loaders_call_priorities_new TO silver_layer.loaders_call_priorities;
