BEGIN;

TRUNCATE TABLE calc.counteragents;

INSERT INTO calc.counteragents 
SELECT *, NOW() as updated_ad
FROM stage.counteragents;

COMMIT;