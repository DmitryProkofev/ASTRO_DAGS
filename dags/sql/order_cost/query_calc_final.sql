BEGIN;

DELETE FROM calc.order_cost_final;

INSERT INTO calc.order_cost_final
SELECT NULL::text AS "UID_Расходника",
    NULL::text AS "EXORDER",
    cs."UID_Заказа",
    cs."ORDER",
    cs."DOCUMENT",
    NULL::numeric AS "Q",
    NULL::double precision AS "QSUM",
    cs."DATEOR",
        CASE
            WHEN
            CASE
                WHEN cs."DOCUMENT" ~~ '%14.03%'::text THEN 1
                ELSE NULL::integer
            END IS NULL THEN cs."DATEOR" + '10 days'::interval
            ELSE cs."DATEOR" + '6 days'::interval
        END AS "PLANDATEEXOR",
        CASE
            WHEN
            CASE
                WHEN cs."DOCUMENT" ~~ '%14.03%'::text THEN 1
                ELSE NULL::integer
            END IS NULL THEN cs."DATEOR" + '11 days'::interval
            ELSE cs."DATEOR" + '7 days'::interval
        END AS "PLANDATESHIP",
    cs."DATEFACT",
    cs."DATEWISH",
        CASE
            WHEN cs."DOCUMENT" ~~ '%14.03%'::text THEN 'консигнация'::text
            ELSE NULL::text
        END AS "консигнация",
        CASE
            WHEN cs."DOCUMENT" !~~ '%14.03%'::text THEN 'заказ'::text
            ELSE NULL::text
        END AS "заказ",
        CASE
            WHEN cs."DOCUMENT" ~~ '%14.03%'::text THEN 'консигнация'::text
            WHEN cs."DOCUMENT" !~~ '%14.03%'::text THEN 'заказ'::text
            ELSE NULL::text
        END AS "Консиг/заказ",
    CURRENT_DATE::timestamp without time zone - cs."DATEOR" AS "Все",
    (EXTRACT(epoch FROM CURRENT_DATE)::double precision - EXTRACT(epoch FROM cs."DATEOR")::double precision) / (24 * 60 * 60)::double precision AS "ВСЕ",
    NULL::interval AS "На 1 ед ВСЕ",
    NULL::double precision AS "На 1 ед ВСЕ1",
    NULL::interval AS "На 1 ед хран ВСЕ",
    NULL::double precision AS "На 1 ед хран ВСЕ1",
    NULL::integer AS "ВСЕ_ПОЗЖЕ_ЗАКАЗ",
    NULL::integer AS "ВСЕ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    NULL::integer AS "ВСЕ_ПОЗЖЕ_КОНСИГ",
    NULL::integer AS "ВСЕ_РАНЬШЕ_В_СРОК_КОНСИГ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 11 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 1
            ELSE NULL::integer
        END AS "ВСЕ_ОПОЗДАНИЕ_ЗАКАЗ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 11 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 1
            ELSE NULL::integer
        END AS "ВСЕ_В_РАБОТЕ_ЗАКАЗ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 7 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 1
            ELSE NULL::integer
        END AS "ВСЕ_ОПОЗДАНИЕ_КОНСИГ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 7 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 1
            ELSE NULL::integer
        END AS "ВСЕ_В_РАБОТЕ_КОНСИГ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 11 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 'в работе с опозданием'::text
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 11 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 'в работе в срок'::text
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 7 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 'в работе с опозданием'::text
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 7 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 'в работе в срок'::text
            ELSE NULL::text
        END AS "Время консиг/заказ ВСЕ",
    CURRENT_DATE::timestamp without time zone - cs."DATEOR" AS "ОЗЧ",
    (EXTRACT(epoch FROM CURRENT_DATE)::double precision - EXTRACT(epoch FROM cs."DATEOR")::double precision) / (24 * 60 * 60)::double precision AS "озч",
    NULL::interval AS "На 1 ед ОЗЧ",
    NULL::double precision AS "На 1 ед ОЗЧ1",
    NULL::interval AS "На 1 ед хран ОЗЧ",
    NULL::double precision AS "На 1 ед хран ОЗЧ1",
    NULL::integer AS "ОЗЧ_ПОЗЖЕ_ЗАКАЗ",
    NULL::integer AS "ОЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    NULL::integer AS "ОЗЧ_ПОЗЖЕ_КОНСИГ",
    NULL::integer AS "ОЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 10 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 1
            ELSE NULL::integer
        END AS "ОЗЧ_ОПОЗДАНИЕ_ЗАКАЗ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 10 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 1
            ELSE NULL::integer
        END AS "ОЗЧ_В_РАБОТЕ_ЗАКАЗ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 6 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 1
            ELSE NULL::integer
        END AS "ОЗЧ_ОПОЗДАНИЕ_КОНСИГ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 6 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 1
            ELSE NULL::integer
        END AS "ОЗЧ_В_РАБОТЕ_КОНСИГ",
        CASE
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 10 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 'в работе с опозданием'::text
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 10 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NULL THEN 'в работе в срок'::text
            WHEN (CURRENT_DATE - cs."DATEOR"::date) > 6 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 'в работе с опозданием'::text
            WHEN (CURRENT_DATE - cs."DATEOR"::date) <= 6 AND
            CASE
                WHEN cs."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                ELSE NULL::text
            END IS NOT NULL THEN 'в работе в срок'::text
            ELSE NULL::text
        END AS "Время консиг/заказ ОЗЧ",
    NULL::interval AS "СЗЧ",
    NULL::double precision AS "сзч",
    NULL::interval AS "На 1 ед СЗЧ",
    NULL::double precision AS "На 1 ед СЗЧ1",
    NULL::interval AS "На 1 ед хран СЗЧ",
    NULL::double precision AS "На 1 ед хран СЗЧ1",
    NULL::integer AS "СЗЧ_ПОЗЖЕ_ЗАКАЗ",
    NULL::integer AS "СЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    NULL::integer AS "СЗЧ_ПОЗЖЕ_КОНСИГ",
    NULL::integer AS "СЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
        CASE
            WHEN a."UID_Заказа" IS NULL THEN 1
            ELSE NULL::integer
        END AS "СЗЧ_В_РАБОТЕ",
        CASE
            WHEN a."UID_Заказа" IS NULL THEN 'не приступали к комплетованию'::text
            ELSE NULL::text
        END AS "Время консиг/заказ СЗЧ",
        CASE
            WHEN a."UID_Заказа" IS NULL THEN 'открыт'::text
            ELSE 'закрыт'::text
        END AS "ОТКРЫТ/ЗАКРЫТ",
    NOW() as updated_ad
   FROM bi_data.clientorder_subdivision cs
     LEFT JOIN ( SELECT o."UID_Заказа"
           FROM calc.order_cost_spares o
          GROUP BY o."UID_Заказа") a ON a."UID_Заказа" = cs."UID_Заказа"
  WHERE a."UID_Заказа" IS NULL
UNION ALL
 SELECT o."UID_Расходника",
    o."EXORDER",
    o."UID_Заказа",
    o."ORDER",
    o."DOCUMENT",
    o.q AS "Q",
    o.qsum AS "QSUM",
    o."DATEOR",
    o."DATEEXOR" AS "PLANDATEEXOR",
    o."DATESHIP" AS "PLANDATESHIP",
    o."DATEFACT",
    o."DATEWISH",
    o."консигнация",
    o."заказ",
    o."Консиг/заказ",
    o."Все",
    (EXTRACT(epoch FROM o."DATESHIP")::double precision - EXTRACT(epoch FROM o."DATEOR")::double precision) / (24 * 60 * 60)::double precision AS "ВСЕ",
    o."На 1 ед ВСЕ",
    (EXTRACT(epoch FROM o."DATESHIP")::double precision - EXTRACT(epoch FROM o."DATEOR")::double precision) / (24 * 60 * 60)::double precision / o.q::double precision AS "На 1 ед ВСЕ1",
    o."На 1 ед хран ВСЕ",
    (EXTRACT(epoch FROM o."DATESHIP")::double precision - EXTRACT(epoch FROM o."DATEOR")::double precision) / (24 * 60 * 60)::double precision / o.qsum AS "На 1 ед хран ВСЕ1",
    o."ВСЕ_ПОЗЖЕ_ЗАКАЗ",
    o."ВСЕ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    o."ВСЕ_ПОЗЖЕ_КОНСИГ",
    o."ВСЕ_РАНЬШЕ_В_СРОК_КОНСИГ",
    NULL::integer AS "ВСЕ_ОПОЗДАНИЕ_ЗАКАЗ",
    NULL::integer AS "ВСЕ_В_РАБОТЕ_ЗАКАЗ",
    NULL::integer AS "ВСЕ_ОПОЗДАНИЕ_КОНСИГ",
    NULL::integer AS "ВСЕ_В_РАБОТЕ_КОНСИГ",
    o."Время консиг/заказ ВСЕ",
    o."ОЗЧ",
    (EXTRACT(epoch FROM o."DATEEXOR")::double precision - EXTRACT(epoch FROM o."DATEOR")::double precision) / (24 * 60 * 60)::double precision AS "озч",
    o."На 1 ед ОЗЧ",
    (EXTRACT(epoch FROM o."DATEEXOR")::double precision - EXTRACT(epoch FROM o."DATEOR")::double precision) / (24 * 60 * 60)::double precision / o.q::double precision AS "На 1 ед ОЗЧ1",
    o."На 1 ед хран ОЗЧ",
    (EXTRACT(epoch FROM o."DATEEXOR")::double precision - EXTRACT(epoch FROM o."DATEOR")::double precision) / (24 * 60 * 60)::double precision / o.qsum AS "На 1 ед хран ОЗЧ1",
    o."ОЗЧ_ПОЗЖЕ_ЗАКАЗ",
    o."ОЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    o."ОЗЧ_ПОЗЖЕ_КОНСИГ",
    o."ОЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
    NULL::integer AS "ОЗЧ_ОПОЗДАНИЕ_ЗАКАЗ",
    NULL::integer AS "ОЗЧ_В_РАБОТЕ_ЗАКАЗ",
    NULL::integer AS "ОЗЧ_ОПОЗДАНИЕ_КОНСИГ",
    NULL::integer AS "ОЗЧ_В_РАБОТЕ_КОНСИГ",
    o."Время консиг/заказ ОЗЧ",
    o."СЗЧ",
    (EXTRACT(epoch FROM o."DATESHIP")::double precision - EXTRACT(epoch FROM o."DATEEXOR")::double precision) / (24 * 60 * 60)::double precision AS "сзч",
    o."На 1 ед СЗЧ",
    (EXTRACT(epoch FROM o."DATESHIP")::double precision - EXTRACT(epoch FROM o."DATEEXOR")::double precision) / (24 * 60 * 60)::double precision / o.q::double precision AS "На 1 ед СЗЧ1",
    o."На 1 ед хран СЗЧ",
    (EXTRACT(epoch FROM o."DATESHIP")::double precision - EXTRACT(epoch FROM o."DATEEXOR")::double precision) / (24 * 60 * 60)::double precision / o.qsum AS "На 1 ед хран СЗЧ1",
    o."СЗЧ_ПОЗЖЕ_ЗАКАЗ",
    o."СЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    o."СЗЧ_ПОЗЖЕ_КОНСИГ",
    o."СЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
    NULL::integer AS "СЗЧ_В_РАБОТЕ",
    o."Время консиг/заказ СЗЧ",
    'закрыт'::text AS "ОТКРЫТ/ЗАКРЫТ",
    NOW() as updated_ad
   FROM calc.order_cost_spares o;

COMMIT;