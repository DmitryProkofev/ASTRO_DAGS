--убираем возможные дубликаты, null, не авторизованных и разрабов/админов - делать на уровне записи в таблицу фактов

#TODO на этом сллое происходит дедубликация через оконную функцию




CREATE TABLE silver_layer.pa_oper_new engine = MergeTree
ORDER BY
idwork AS
SELECT
	idwork,
	operator,
	opersurn,
	opername,
	operpatr,
	idws,
	opergrup,
	operlevel,
	operstemp,
	operpass,
	idprof,
	proflevel,
	email,
	operbarcode,
	opertabnum,
	ikodsotrudnik,
	tg_id,
	is_loader,
	`uuid`,
	dtmodified,
	nvciduser,
	now('Europe/Samara') AS update_etl
	FROM
	(
SELECT
		*,
		ROW_NUMBER() OVER (
            PARTITION BY idwork
	ORDER BY
		update_etl DESC
        ) AS rn
	FROM
		bronze_layer.pa_oper) sub
where rn = 1;




RENAME TABLE silver_layer.pa_oper TO silver_layer.pa_oper_old;


RENAME TABLE silver_layer.pa_oper_new TO silver_layer.pa_oper;







