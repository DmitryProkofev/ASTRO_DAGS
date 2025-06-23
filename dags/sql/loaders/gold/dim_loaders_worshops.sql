#

CREATE TABLE gold_layer.dim_loaders_workshops
(
    srgt_id UInt32,
    id UInt32,
    workshop_name String,
    workshop_description String,
    updated_at DateTime,
    updated_etl DateTime('Europe/Samara') DEFAULT now('Europe/Samara')
) 
ENGINE = ReplacingMergeTree(updated_etl)
ORDER BY id;



INSERT
	INTO
	gold_layer.dim_loaders_workshops
(srgt_id,
	id,
	workshop_name,
	workshop_description,
	updated_at)
SELECT
	(
	SELECT
		if(max(srgt_id) IS NULL, 0, max(srgt_id))
	FROM
		gold_layer.dim_loaders_workshops) + 
    rowNumberInAllBlocks() + 1 AS srgt_id,
	id,
	workshop_name,
	workshop_description,
	updated_at
FROM
	silver_layer.loaders_workshops;



