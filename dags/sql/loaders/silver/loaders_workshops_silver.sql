-- TODO на этом сллое происходит дедубликация через оконную функцию


CREATE TABLE silver_layer.loaders_workshops_new engine = MergeTree
ORDER BY
id AS
select
	id,
	workshop_name,
	workshop_description,
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
		bronze_layer.loaders_workshops) sub
where
	rn = 1;


--RENAME TABLE silver_layer.loaders_workshops TO silver_layer.loaders_workshops_old;
--
--
--RENAME TABLE silver_layer.loaders_workshops_new TO silver_layer.loaders_workshops;
