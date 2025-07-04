--- проверка на сущестование новых строк

SELECT
    1
FROM
    postgresql(
        '10.1.11.17:5432',
        'AGRO',
        'loader_calls',
        'airflow_etl',
        'airpegas',
        'public'
    )
WHERE
    close_time IS NOT NULL
    AND toUnixTimestamp(updated_at) > (
        SELECT
            coalesce(max(toUnixTimestamp(updated_at)), 0)
        FROM gold_layer.fct_loaders_calls
    )
LIMIT 1

