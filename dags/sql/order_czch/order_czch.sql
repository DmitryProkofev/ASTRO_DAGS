BEGIN;

TRUNCATE TABLE calc.order_szch;

INSERT INTO calc.order_szch 
SELECT *, NOW() as updated_ad
FROM stage.order_szch;

COMMIT;