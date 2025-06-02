--убираем возможные дубликаты, null, не авторизованных и разрабов/админов - делать на уровне записи в таблицу фактов


--Теоретически таблица измерений с пользаками может использоваться в других аналитических отчетах, а значит отфильтровывать
-- нужных мне юзеров для определенного аналитического отчета/дашборда кажется излишним, ибо для друго отчета будут уже другие фильтры,
-- потому фильтрацию выполнять уже на моменте записи в таблицу фактов

--- тут нет особой обработки потому кидать сразу в gold_layer dimension table

CREATE TABLE IF NOT EXISTS gold_layer.pa_oper engine=MergeTree order by update_data AS
select surrogate_key,
		tg_id,
		operator,
		is_loader,
		now('Europe/Samara') AS update_data
FROM (		
select
	generateUUIDv4() AS surrogate_key,
	tg_id,
		operator,
		is_loader,
		update_data,
	ROW_NUMBER() OVER (PARTITION BY tg_id
ORDER BY
	update_data DESC) as rn
from
	bronze_layer.pa_oper)
where rn = 1;


--- 
and tg_id is not Null
and is_loader is not null
and tg_id NOT IN (5773698501, 325813539);




CREATE TABLE gold_layer.pa_oper
(	
	id UInt32, 
    tg_id UInt64,
		operator String,
		is_loader UInt8  ,
		update_data DateTime 
)
ENGINE = MergeTree
ORDER BY id;



TRUNCATE TABLE gold_layer.pa_oper;


--- код для автоинкремента
SELECT
    row_number() OVER (ORDER BY idwork) + max_id_in_dim AS surrogate_key,
    *
FROM
(
    SELECT *, (SELECT max(id) FROM gold_layer.pa_oper) AS max_id_in_dim
    FROM bronze_layer.pa_oper
);






