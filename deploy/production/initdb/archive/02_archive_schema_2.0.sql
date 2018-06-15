BEGIN;

/* move rbversion from alert to candidate */
ALTER TABLE candidate ADD rbversion TEXT;
UPDATE candidate SET rbversion = alert.rbversion
    FROM alert WHERE alert.alert_id=candidate.alert_id;
ALTER TABLE alert DROP COLUMN rbversion;

/* add rbversion to prv_candidate */
ALTER TABLE prv_candidate ADD rbversion TEXT;

INSERT INTO versions (alert_version) VALUES (2.0);

COMMIT;

