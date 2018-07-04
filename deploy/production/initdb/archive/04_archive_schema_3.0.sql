BEGIN;

/* 2018-07-04 UT: add fields to candidate */
ALTER TABLE candidate ADD dsnrms FLOAT;
ALTER TABLE candidate ADD ssnrms FLOAT;
ALTER TABLE candidate ADD dsdiff FLOAT;
ALTER TABLE candidate ADD magzpsci FLOAT;
ALTER TABLE candidate ADD magzpsciunc FLOAT;
ALTER TABLE candidate ADD nmatches INTEGER;
ALTER TABLE candidate ADD clrcoeff FLOAT;
ALTER TABLE candidate ADD clrcounc FLOAT;
ALTER TABLE candidate ADD zpclrcov FLOAT;
ALTER TABLE candidate ADD zpmed FLOAT;
ALTER TABLE candidate ADD clrmed FLOAT;
ALTER TABLE candidate ADD clrrms FLOAT;
ALTER TABLE candidate ADD neargaia FLOAT;
ALTER TABLE candidate ADD neargaiabright FLOAT;
ALTER TABLE candidate ADD maggaia FLOAT;
ALTER TABLE candidate ADD maggaiabright FLOAT;

INSERT INTO versions (alert_version) VALUES (3.0);

COMMIT;
