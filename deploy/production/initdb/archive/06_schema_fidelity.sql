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

/* Add back ssnamenr column */
ALTER TABLE candidate ADD ssnamenr text;
ALTER TABLE prv_candidate ADD ssnamenr text;
