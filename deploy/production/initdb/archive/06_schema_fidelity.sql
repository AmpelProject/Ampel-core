/* Various tweaks to comply *exactly* with schema 3.0 */

/* Restore string form of isdiffpos */
ALTER TABLE candidate ALTER COLUMN isdiffpos SET DATA TYPE text USING
	(
	CASE isdiffpos
		WHEN true THEN 't'
		ELSE 'f'
	END
	);
ALTER TABLE prv_candidate ALTER COLUMN isdiffpos SET DATA TYPE text USING
	(
	CASE isdiffpos
		WHEN true THEN 't'
		ELSE 'f'
	END
	);

ALTER TABLE candidate ADD magzpscirms FLOAT;

/* Add back ssnamenr column */
ALTER TABLE candidate ADD ssnamenr text;
ALTER TABLE prv_candidate ADD ssnamenr text;

/* Store rbversion for upper limits as well */
ALTER TABLE upper_limit ADD rbversion text;
