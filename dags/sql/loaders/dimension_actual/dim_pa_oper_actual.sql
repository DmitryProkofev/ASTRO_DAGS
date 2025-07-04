--- вместо оконки решил использовать другой подход - через группировку, ради разнообразия


CREATE TABLE gold_layer.dim_pa_oper_actual_new
ENGINE = MergeTree
ORDER BY id_srgt AS
SELECT
	d.id_srgt,
    d.id_oltp,
    d.operator,
	d.opersurn,
	d.opername,
	d.operpatr,
	d.idws,
	d.opergrup,
	d.operlevel,
	d.operstemp,
	d.operpass,
	d.idprof,
	d.proflevel,
	d.email,
	d.operbarcode,
	d.opertabnum,
	d.ikodsotrudnik,
	d.tg_id,
	d.is_loader,
	d.`uuid`,
	d.dtmodified,
	d.nvciduser,
    now('Europe/Samara') AS update_etl
FROM gold_layer.dim_pa_oper d
INNER JOIN (
    SELECT
        id_oltp,
        max(dtmodified) AS max_updated_at
    FROM gold_layer.dim_pa_oper
    GROUP BY id_oltp
) latest ON d.id_oltp = latest.id_oltp AND d.dtmodified = latest.max_updated_at
COMMENT 'Таблица с актуальными версиями записей';

