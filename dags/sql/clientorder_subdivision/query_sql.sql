BEGIN;

TRUNCATE TABLE calc.clientorder_subdivision;

INSERT INTO calc.clientorder_subdivision
select *, NOW() as updated_ad from stage.clientorder_subdivision;

COMMIT;