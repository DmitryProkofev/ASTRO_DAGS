--- вместо оконки решил использовать другой подход - через группировку, ради разнообразия


CREATE TABLE gold_layer.dim_loaders_reasons_actual_new
ENGINE = MergeTree
ORDER BY id AS
SELECT
	d.srgt_id,
    d.id,
    d.reason,
    d.reason_full,
    d.target_workshop_id,
    d.updated_at,
    now('Europe/Samara') AS update_etl
FROM gold_layer.dim_loaders_reasons d
INNER JOIN (
    SELECT
        id,
        max(updated_at) AS max_updated_at
    FROM gold_layer.dim_loaders_reasons
    GROUP BY id
) latest ON d.id = latest.id AND d.updated_at = latest.max_updated_at
COMMENT 'Таблица с актуальными версиями записей';


