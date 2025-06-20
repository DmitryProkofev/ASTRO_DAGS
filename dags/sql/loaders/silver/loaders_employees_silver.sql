--убираем возможные дубликаты, null, не авторизованных и разрабов/админов - делать на уровне записи в таблицу фактов

#TODO на этом сллое происходит дедубликация через оконную функцию




CREATE TABLE silver_layer.loaders_employes_silver engine = MergeTree
ORDER BY
id AS



SELECT
		*,
		ROW_NUMBER() OVER (
            PARTITION BY id
	ORDER BY
		updated_at DESC
        ) AS rn
	FROM
		bronze_layer.loaders_calls




