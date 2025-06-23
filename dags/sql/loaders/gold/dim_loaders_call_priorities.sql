#

CREATE TABLE gold_layer.dim_loaders_call_priorities
(
    srgt_id UInt32,
    id UInt32,
    priority_name String,
    priority_desc String,
    emojie String,
    updated_at DateTime,
    updated_etl DateTime('Europe/Samara') DEFAULT now('Europe/Samara')
) 
ENGINE = ReplacingMergeTree(updated_etl)
ORDER BY id;




INSERT
	INTO
	gold_layer.dim_loaders_call_priorities
(srgt_id,
	id,
	priority_name,
	priority_desc,
	emojie,
	updated_at)
SELECT
	(
	SELECT
		if(max(srgt_id) IS NULL, 0, max(srgt_id))
	FROM
		gold_layer.dim_loaders_call_priorities) + 
    rowNumberInAllBlocks() + 1 AS srgt_id,
	id,
	priority_name,
	priority_desc,
	emojie,
	updated_at
FROM
	silver_layer.loaders_call_priorities;
