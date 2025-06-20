#

CREATE TABLE gold_layer.dim_loaders_workshops_gold
(
    id UInt32,
    workshop_name String,
    workshop_description String,
    updated_at DateTime,
    updated_etl DateTime DEFAULT now()
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;


#TODO запихнуть оконную функцию для дедубликации

INSERT INTO gold_layer.dim_loaders_workshops_gold
(id, workshop_name, workshop_description, updated_at)



# формирование суррогатного ключа
SELECT 
    (SELECT if(max(id) IS NULL, 0, max(id)) FROM gold_layer.dim_loaders_workshops_gold) + 
    rowNumberInAllBlocks() +1 AS srgt_id,
    id,
    workshop_name,
    workshop_description,
    updated_at
FROM bronze_layer.loaders_workshops_bronze;



