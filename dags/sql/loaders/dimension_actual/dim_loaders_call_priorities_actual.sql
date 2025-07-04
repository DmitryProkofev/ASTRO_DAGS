--- вместо оконки решил использовать другой подход - через группировку, ради разнообразия

CREATE TABLE gold_layer.loaders_call_priorities_actual_new
ENGINE = MergeTree
ORDER BY id AS
SELECT
	d.srgt_id,
    d.id,
    d.priority_name,
    d.priority_desc,
    d.emojie,
    d.updated_at,
    now('Europe/Samara') AS update_etl
FROM gold_layer.dim_loaders_call_priorities d
INNER JOIN (
    SELECT
        id,
        max(updated_at) AS max_updated_at
    FROM gold_layer.dim_loaders_call_priorities
    GROUP BY id
) latest ON d.id = latest.id AND d.updated_at = latest.max_updated_at
COMMENT 'Таблица с актуальными версиями записей';


