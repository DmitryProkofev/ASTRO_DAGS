DECLARE 
    v_error_msg VARCHAR2(4000);
    v_src_count NUMBER;
BEGIN
    -- 1. Проверка: есть ли данные в источнике?
    -- Это защитит от удаления всех записей в целевой таблице, если парсинг упал
    SELECT COUNT(*) INTO v_src_count FROM AIRFLOW.ATS_NEW;
    
    IF v_src_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Таблица источника AIRFLOW.ATS_NEW пуста. Операция прервана во избежание потери данных.');
    END IF;

    -- 2. MERGE (обновление + вставка) по WORK_PHONE
    MERGE INTO DATA_EX.EMPLOYES_ATS_NEW dst
    USING AIRFLOW.ATS_NEW src
    ON (dst."WORK_PHONE" = src."WORK_PHONE")
    WHEN MATCHED THEN
        UPDATE SET 
            dst."NAME" = src."NAME",
            dst."MAIL" = src."MAIL",
            dst."MOBILE" = src."MOBILE",
            dst."UUID" = src."UUID" -- Обновляем и UUID, если он изменился
    WHEN NOT MATCHED THEN
        INSERT ("WORK_PHONE", "NAME", "MAIL", "MOBILE", "UUID")
        VALUES (src."WORK_PHONE", src."NAME", src."MAIL", src."MOBILE", src."UUID");

    -- 3. DELETE (удаление записей, которых нет в источнике)
    DELETE FROM DATA_EX.EMPLOYES_ATS_NEW dst
    WHERE NOT EXISTS (
        SELECT 1 FROM AIRFLOW.ATS_NEW src 
        WHERE src."WORK_PHONE" = dst."WORK_PHONE"
    );

    -- 4. Фиксируем изменения
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;
        ROLLBACK;
        -- Важно: выбрасываем ошибку, чтобы Airflow увидел падение задачи (Failed)
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка при обновлении ATS: ' || v_error_msg);
END;