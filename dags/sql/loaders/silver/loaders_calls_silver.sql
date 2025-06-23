#TODO запихнуть DQ между слоем silver и gold

-- выполняем дедубликацию через оконную функцию,
-- фильтруем тестовые вызовы по id телеги


create table silver_layer.loaders_calls_new engine=MergeTree order by id as 
SELECT
	id,
	open_time,
	customer_id,
	workshop_id,
	call_reason_id,
	comment,
	loader_id,
	taken_time,
	close_time,
	priority,
	container_qty,
	updated_at,
	now('Europe/Samara') AS update_etl
FROM
	(
	SELECT
		*,
		ROW_NUMBER() OVER (
            PARTITION BY id
	ORDER BY
		updated_at DESC
        ) AS rn
	FROM
		bronze_layer.loaders_calls
	WHERE
		close_time IS NOT NULL
		AND 
     customer_id NOT IN (5773698501, 325813539)
		AND loader_id NOT IN (5773698501, 325813539)
) sub
WHERE
	rn = 1;


RENAME TABLE silver_layer.loaders_calls TO silver_layer.loaders_calls_old;


RENAME TABLE silver_layer.loaders_calls_new TO silver_layer.loaders_calls;














