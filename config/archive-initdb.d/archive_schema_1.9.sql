BEGIN;

/* note: we do not store the publisher field */
ALTER TABLE alert ADD schemavsn TEXT;
ALTER TABLE alert ADD rbversion TEXT;

INSERT INTO versions (alert_version) VALUES (1.9);

COMMIT;

