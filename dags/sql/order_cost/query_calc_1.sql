BEGIN;

DELETE FROM calc.order_cost_spares;

INSERT INTO calc.order_cost_spares
SELECT a1."UID_Расходника",
    a1."EXORDER",
    a1."UID_Заказа",
    a1."ORDER",
    a1."DOCUMENT",
    a1.q,
    a1.qsum,
    a1."DATEOR",
    a1."DATEEXOR",
    a1."DATESHIP",
    a1."DATEFACT",
    a1."DATEWISH",
    a1."консигнация",
    a1."заказ",
    a1."Консиг/заказ",
    a1."Все",
    a1."На 1 ед ВСЕ",
    a1."На 1 ед хран ВСЕ",
    a1."ВСЕ_ПОЗЖЕ_ЗАКАЗ",
    a1."ВСЕ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    a1."ВСЕ_ПОЗЖЕ_КОНСИГ",
    a1."ВСЕ_РАНЬШЕ_В_СРОК_КОНСИГ",
    a1."Время консиг/заказ ВСЕ",
    a1."ОЗЧ",
    a1."На 1 ед ОЗЧ",
    a1."На 1 ед хран ОЗЧ",
    a1."ОЗЧ_ПОЗЖЕ_ЗАКАЗ",
    a1."ОЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    a1."ОЗЧ_ПОЗЖЕ_КОНСИГ",
    a1."ОЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
    a1."Время консиг/заказ ОЗЧ",
    a1."СЗЧ",
    a1."На 1 ед СЗЧ",
    a1."На 1 ед хран СЗЧ",
    a1."СЗЧ_ПОЗЖЕ_ЗАКАЗ",
    a1."СЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
    a1."СЗЧ_ПОЗЖЕ_КОНСИГ",
    a1."СЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
    a1."Время консиг/заказ СЗЧ",
    NOW() as updated_ad
   FROM ( SELECT a."UID_Расходника",
            a."EXORDER",
            a."UID_Заказа",
            a."ORDER",
            a."DOCUMENT",
            a.q,
            a.qsum,
            a."DATEOR",
            a."DATEEXOR",
            a."DATESHIP",
            a."DATEFACT",
            a."DATEWISH",
                CASE
                    WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                    ELSE NULL::text
                END AS "консигнация",
                CASE
                    WHEN a."DOCUMENT" !~~ '%№14.03%'::text THEN 'заказ'::text
                    ELSE NULL::text
                END AS "заказ",
                CASE
                    WHEN a."DOCUMENT" !~~ '%№14.03%'::text THEN 'заказ'::text
                    WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                    ELSE NULL::text
                END AS "Консиг/заказ",
            a."DATESHIP" - a."DATEOR" AS "Все",
            (a."DATESHIP" - a."DATEOR") / a.q::double precision AS "На 1 ед ВСЕ",
            (a."DATESHIP" - a."DATEOR") / a.qsum AS "На 1 ед хран ВСЕ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) > 11 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NULL THEN 1
                    ELSE NULL::integer
                END AS "ВСЕ_ПОЗЖЕ_ЗАКАЗ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) <= 11 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NULL THEN 1
                    ELSE NULL::integer
                END AS "ВСЕ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) > 7 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NOT NULL THEN 1
                    ELSE NULL::integer
                END AS "ВСЕ_ПОЗЖЕ_КОНСИГ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) <= 7 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NOT NULL THEN 1
                    ELSE NULL::integer
                END AS "ВСЕ_РАНЬШЕ_В_СРОК_КОНСИГ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) > 11 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NULL THEN 'выполнен позже'::text
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) <= 11 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NULL THEN 'выполнен раньше или в срок'::text
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) > 7 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NOT NULL THEN 'выполнен позже'::text
                    WHEN (a."DATESHIP"::date - a."DATEOR"::date) <= 7 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NOT NULL THEN 'выполнен раньше или в срок'::text
                    ELSE NULL::text
                END AS "Время консиг/заказ ВСЕ",
            a."DATEEXOR" - a."DATEOR" AS "ОЗЧ",
            (a."DATEEXOR" - a."DATEOR") / a.q::double precision AS "На 1 ед ОЗЧ",
            (a."DATEEXOR" - a."DATEOR") / a.qsum AS "На 1 ед хран ОЗЧ",
                CASE
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) > 10 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NULL THEN 1
                    ELSE NULL::integer
                END AS "ОЗЧ_ПОЗЖЕ_ЗАКАЗ",
                CASE
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) <= 10 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NULL THEN 1
                    ELSE NULL::integer
                END AS "ОЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
                CASE
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) > 6 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NOT NULL THEN 1
                    ELSE NULL::integer
                END AS "ОЗЧ_ПОЗЖЕ_КОНСИГ",
                CASE
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) <= 6 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NOT NULL THEN 1
                    ELSE NULL::integer
                END AS "ОЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
                CASE
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) > 10 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NULL THEN 'выполнен позже'::text
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) <= 10 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NULL THEN 'выполнен раньше или в срок'::text
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) > 6 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NOT NULL THEN 'выполнен позже'::text
                    WHEN (a."DATEEXOR"::date - a."DATEOR"::date) <= 6 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NOT NULL THEN 'выполнен раньше или в срок'::text
                    ELSE NULL::text
                END AS "Время консиг/заказ ОЗЧ",
            a."DATESHIP" - a."DATEEXOR" AS "СЗЧ",
            (a."DATESHIP" - a."DATEEXOR") / a.q::double precision AS "На 1 ед СЗЧ",
            (a."DATESHIP" - a."DATEEXOR") / a.qsum AS "На 1 ед хран СЗЧ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date) > 1 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NULL THEN 1
                    ELSE NULL::integer
                END AS "СЗЧ_ПОЗЖЕ_ЗАКАЗ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date) <= 1 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NULL THEN 1
                    ELSE NULL::integer
                END AS "СЗЧ_РАНЬШЕ_В_СРОК_ЗАКАЗ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date) > 1 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NOT NULL THEN 1
                    ELSE NULL::integer
                END AS "СЗЧ_ПОЗЖЕ_КОНСИГ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date) <= 1 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 1
                        ELSE NULL::integer
                    END IS NOT NULL THEN 1
                    ELSE NULL::integer
                END AS "СЗЧ_РАНЬШЕ_В_СРОК_КОНСИГ",
                CASE
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date)::numeric > 0.08 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NULL THEN 'выполнен позже'::text
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date)::numeric <= 0.08 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NULL THEN 'выполнен раньше или в срок'::text
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date) > 1 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NOT NULL THEN 'выполнен позже'::text
                    WHEN (a."DATESHIP"::date - a."DATEEXOR"::date) <= 1 AND
                    CASE
                        WHEN a."DOCUMENT" ~~ '%№14.03%'::text THEN 'консигнация'::text
                        ELSE NULL::text
                    END IS NOT NULL THEN 'выполнен раньше или в срок'::text
                    ELSE NULL::text
                END AS "Время консиг/заказ СЗЧ"
           FROM ( SELECT min(b."UID_Cost") AS "UID_Расходника",
                    min(b."EXORDER") AS "EXORDER",
                    b."UID_Order" AS "UID_Заказа",
                    b."ORDER",
                    b."DOCUMENT",
                    sum(b.q) AS q,
                    sum(b.qsum) AS qsum,
                    min(b."DATEOR") AS "DATEOR",
                    min(b."DATEEXOR") AS "DATEEXOR",
                    max(b."DATESHIP") AS "DATESHIP",
                    max(b."DATEFACT") AS "DATEFACT",
                    max(b."DATEWISH") AS "DATEWISH"
                   FROM ( SELECT oc."UID_Cost",
                            oc."EXORDER",
                            oc."UID_Order",
                            oc."ORDER",
                            oc."DOCUMENT",
                            count(oc."Nomen") AS q,
                            sum(oc."Count") AS qsum,
                            min(oc."DATEOR") AS "DATEOR",
                            min(oc."DATEEXOR") AS "DATEEXOR",
                            min(oc."DATESHIP") AS "DATESHIP",
                            min(oc."DATEFACT") AS "DATEFACT",
                            min(oc."DATEWISH") AS "DATEWISH"
                           FROM airflow_data.order_cost oc
                          GROUP BY oc."UID_Cost", oc."EXORDER", oc."UID_Order", oc."ORDER", oc."DOCUMENT") b
                  GROUP BY b."UID_Order", b."ORDER", b."DOCUMENT") a) a1;

COMMIT;