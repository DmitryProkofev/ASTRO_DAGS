#TODO здесь выполнить через оконную функцию


--create table silver_layer.loaders_calls engine=MergeTree order by id as 
INSERT INTO silver_layer.loaders_calls
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
	now('Europe/Samara') AS update_data,
	rn
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY update_data DESC
        ) AS rn
    FROM bronze_layer.loaders_calls
    WHERE close_time IS NOT NULL
      AND id > (
          SELECT max(id)
          FROM silver_layer.loaders_calls
      )
) sub
WHERE rn = 1;



TRUNCATE TABLE silver_layer.loaders_calls;

