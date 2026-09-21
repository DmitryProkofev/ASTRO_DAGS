DECLARE v_null_keys_count NUMBER;

v_dup_phones_count NUMBER;

v_dup_uuids_count NUMBER;

v_null_details_count NUMBER;

BEGIN
-- 1. Проверка на NULL в ключевых полях
SELECT
    COUNT(*) INTO v_null_keys_count
FROM
    AIRFLOW.ATS_NEW
WHERE
    "WORK_PHONE" IS NULL
    and "MOBILE" is Null;

IF v_null_keys_count > 0 THEN RAISE_APPLICATION_ERROR (
    -20001,
    'FAIL: Обнаружено ' || v_null_keys_count || ' записей с пустым WORK_PHONE или UUID.'
);

END IF;

-- 2. Проверка на дубликаты WORK_PHONE
SELECT
    COUNT(*) INTO v_dup_phones_count
FROM
    (
        SELECT
            "WORK_PHONE"
        FROM
            AIRFLOW.ATS_NEW
        GROUP BY
            "WORK_PHONE"
        HAVING
            COUNT(*) > 1
    );

IF v_dup_phones_count > 0 THEN RAISE_APPLICATION_ERROR (
    -20002,
    'FAIL: Обнаружены дубликаты WORK_PHONE (' || v_dup_phones_count || ' шт).'
);

END IF;

-- 3. Проверка на дубликаты UUID
SELECT
    COUNT(*) INTO v_dup_uuids_count
FROM
    (
        SELECT
            "UUID"
        FROM
            AIRFLOW.ATS_NEW
        GROUP BY
            "UUID"
        HAVING
            COUNT(*) > 1
            and UUID is not Null
    );

IF v_dup_uuids_count > 0 THEN RAISE_APPLICATION_ERROR (
    -20003,
    'FAIL: Обнаружены дубликаты UUID (' || v_dup_uuids_count || ' шт).'
);

END IF;

-- -- 4. Проверка заполненности деталей при наличии UUID
-- SELECT
--     COUNT(*) INTO v_null_details_count
-- FROM
--     AIRFLOW.ATS_NEW
-- WHERE
--     "UUID" IS NOT NULL
--     AND (
--         "NAME" IS NULL
--         OR "MAIL" IS NULL
--         OR "MOBILE" IS NULL
--     );
-- IF v_null_details_count > 0 THEN RAISE_APPLICATION_ERROR (
--     -20004,
--     'FAIL: У ' || v_null_details_count || ' записей с UUID отсутствуют NAME, MAIL или MOBILE.'
-- );
-- END IF;
-- Если мы дошли сюда, значит все проверки пройдены
DBMS_OUTPUT.PUT_LINE ('SUCCESS: Все проверки качества данных пройдены.');

END;