
--CREATE TABLE gold_layer.dim_loaders_reasons
--(
--    srgt_id UInt32,
--    id UInt32,
--    reason String,
--    reason_full String,
--    target_workshop_id UInt16,
--    updated_at DateTime,
--    updated_etl DateTime('Europe/Samara') DEFAULT now('Europe/Samara')
--) 
--ENGINE = ReplacingMergeTree(updated_etl)
--ORDER BY id;


INSERT
	INTO
	gold_layer.dim_loaders_reasons
(srgt_id,
	id,
	reason,
	reason_full,
	target_workshop_id,
	updated_at
	)
SELECT
	(
	SELECT
		if(max(srgt_id) IS NULL, 0, max(srgt_id))
	FROM
		gold_layer.dim_loaders_reasons) + 
    rowNumberInAllBlocks() + 1 AS srgt_id,
	id,
	reason,
	reason_full,
	target_workshop_id,
	updated_at
FROM
	silver_layer.loaders_reasons;