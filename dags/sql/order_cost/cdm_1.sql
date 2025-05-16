BEGIN;

DELETE FROM cdm.order_finebi;

INSERT INTO cdm.order_finebi
select of2.*, concat((case when of2."ВСЕ"<1 then 'меньше 1 дня' else null end), (case when trunc(of2."ВСЕ")=0 then null else trunc(of2."ВСЕ") end)) days,
concat((case when (case when of2."ВСЕ"<1 then extract(hour from of2."Все") else null end) = 0 then 'меньше 1 часа' else null end),
case when (case when of2."ВСЕ"<1 then extract(hour from of2."Все") else null end) = 0 then null else (case when of2."ВСЕ"<1 then extract(hour from of2."Все") else null end) end) hours,
case when of2."ОТКРЫТ/ЗАКРЫТ" like 'открыт' and of2."DATEOR" >= current_date-interval '90 days' then 'выходит за квартал' else null end as "Выходит",
case when of2."ОТКРЫТ/ЗАКРЫТ" like 'открыт' and of2."DATEOR" < current_date-interval '90 days' then 'открытые' else null end as "В рамках",
case when (case 
	when of2."ОТКРЫТ/ЗАКРЫТ" like 'открыт' and of2."DATEOR" < current_date-interval '90 days' then 'работа прекращена' 
	when of2."ОТКРЫТ/ЗАКРЫТ" like 'открыт' and of2."DATEOR" >= current_date-interval '90 days' then 'работа ведется'
	else null end) is null then of2."ОТКРЫТ/ЗАКРЫТ" else (case 
	when of2."ОТКРЫТ/ЗАКРЫТ" like 'открыт' and of2."DATEOR" < current_date-interval '90 days' then 'работа прекращена' 
	when of2."ОТКРЫТ/ЗАКРЫТ" like 'открыт' and of2."DATEOR" >= current_date-interval '90 days' then 'работа ведется'
	else null end) end as "Статус",
case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then 'нет желаемой даты отгрузки' else 'есть желаемая дата отгрузки' end as "Желдата",
case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" - of2."DATEFACT" end as "разность",
(EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision AS "разндней",
case when ((EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision) < 0 then 'отгрузили позже'
when ((EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision) > 0 then 'отгрузили раньше'
when ((EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision) = 0 then 'отгрузили в тот же день'
else null end as da,
case when ((EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision) < 0 then 1 else null end as slaafter,
case when ((EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision) > 0 then 1 else null end as slabefore,
case when ((EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEWISH" end))::double precision - EXTRACT(epoch FROM (case when of2."DATEWISH" = '0001-01-01 00:00:00.000' then null else of2."DATEFACT" end))::double precision) / (24 * 60 * 60)::double precision) = 0 then 1 else null end as slaok,
case when of2."сзч" = 0 then 1 else null end as nulldays,
NOW() as updated_ad
from calc.order_cost_final of2;

COMMIT;