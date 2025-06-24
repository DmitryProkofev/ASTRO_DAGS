-- TODO на этом сллое происходит дедубликация через оконную функцию


CREATE TABLE silver_layer.loaders_reasons_new engine = MergeTree
ORDER BY
id AS
SELECT
	id,
	reason,
	reason_full,
	target_workshop_id,
	updated_at,
	now('Europe/Samara') AS update_etl
from(
	SELECT
		*,
		ROW_NUMBER() OVER (
            PARTITION BY id
ORDER BY
		update_etl DESC
        ) AS rn
FROM
		bronze_layer.loaders_reasons) sub
where rn = 1;

--
--RENAME TABLE silver_layer.loaders_reasons TO silver_layer.loaders_reasons_old;
--
--
--RENAME TABLE silver_layer.loaders_reasons_new TO silver_layer.loaders_reasons;




