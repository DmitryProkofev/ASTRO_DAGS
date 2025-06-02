BEGIN;

TRUNCATE TABLE calc.realization;

INSERT INTO calc.realization 
SELECT *, NOW() as updated_ad
FROM stage.realization;

COMMIT;