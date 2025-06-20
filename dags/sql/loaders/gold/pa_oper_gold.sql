#

CREATE TABLE gold_layer.dim_pa_oper_gold
(
    id_srgt UInt32,
    id_oltp UInt32,
    operator String,
    opersurn String,
    opername String,
    operpatr String,
    idws UInt32,
    opergrup String,
    operlevel UInt32,
    operstemp String,
    operpass String,
    idprof UInt32,
    proflevel UInt32,
    email String,
    operbarcode String,
    opertabnum String,
    ikodsotrudnik UInt32,
    tg_id UInt32,
    is_loader UInt16,
    uuid String,
    dtmodified DateTime,
    nvciduser String,
    updated_etl DateTime DEFAULT now()
) 
ENGINE = ReplacingMergeTree(updated_etl)
ORDER BY id_srgt;




INSERT INTO gold_layer.dim_pa_oper_gold
(id_srgt, id_oltp, operator, opersurn, opername, operpatr, 
    idws, opergrup, operlevel, operstemp, operpass, idprof, 
    proflevel, email, operbarcode, opertabnum, ikodsotrudnik, 
    tg_id, is_loader, uuid, dtmodified, nvciduser)
SELECT
	(
	SELECT
		if(max(id_oltp) IS NULL, 0, max(id_oltp))
	FROM
		gold_layer.dim_pa_oper_gold) + 
    rowNumberInAllBlocks() + 1 AS id_srgt,
	idwork AS id_oltp,
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
	nvciduser
FROM
	silver_layer.pa_oper_silver;

