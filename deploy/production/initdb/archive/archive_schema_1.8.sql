CREATE TYPE cutout_kind AS ENUM ('science', 'template', 'difference');
CREATE TABLE versions (
	version_id SERIAL NOT NULL, 
	alert_version TEXT NOT NULL, 
	PRIMARY KEY (version_id), 
	UNIQUE (alert_version)
)

;
CREATE TABLE alert (
	alert_id SERIAL NOT NULL, 
	candid BIGINT NOT NULL, 
	programid INTEGER NOT NULL, 
	"objectId" VARCHAR(12) NOT NULL, 
	partition_id INTEGER NOT NULL, 
	ingestion_time BIGINT NOT NULL, 
	jd DOUBLE PRECISION NOT NULL, 
	PRIMARY KEY (alert_id), 
	UNIQUE (candid, programid)
)

;CREATE INDEX alert_playback ON alert (partition_id, jd);
CREATE TABLE prv_candidate (
	prv_candidate_id SERIAL NOT NULL, 
	jd DOUBLE PRECISION, 
	fid INTEGER, 
	pid BIGINT, 
	diffmaglim FLOAT, 
	programid INTEGER, 
	candid BIGINT, 
	isdiffpos BOOLEAN, 
	tblid BIGINT, 
	nid INTEGER, 
	rcid INTEGER, 
	field INTEGER, 
	xpos FLOAT, 
	ypos FLOAT, 
	ra DOUBLE PRECISION, 
	dec DOUBLE PRECISION, 
	magpsf FLOAT, 
	sigmapsf FLOAT, 
	chipsf FLOAT, 
	magap FLOAT, 
	sigmagap FLOAT, 
	distnr FLOAT, 
	magnr FLOAT, 
	sigmagnr FLOAT, 
	chinr FLOAT, 
	sharpnr FLOAT, 
	sky FLOAT, 
	magdiff FLOAT, 
	fwhm FLOAT, 
	classtar FLOAT, 
	mindtoedge FLOAT, 
	magfromlim FLOAT, 
	seeratio FLOAT, 
	aimage FLOAT, 
	bimage FLOAT, 
	aimagerat FLOAT, 
	bimagerat FLOAT, 
	elong FLOAT, 
	nneg INTEGER, 
	nbad INTEGER, 
	rb FLOAT, 
	ssdistnr FLOAT, 
	ssmagnr FLOAT, 
	sumrat FLOAT, 
	magapbig FLOAT, 
	sigmagapbig FLOAT, 
	ranr DOUBLE PRECISION, 
	decnr DOUBLE PRECISION, 
	scorr DOUBLE PRECISION, 
	PRIMARY KEY (prv_candidate_id), 
	UNIQUE (candid, programid, pid)
)

;
CREATE TABLE upper_limit (
	upper_limit_id SERIAL NOT NULL, 
	jd DOUBLE PRECISION, 
	fid INTEGER, 
	pid BIGINT, 
	diffmaglim FLOAT, 
	programid INTEGER, 
	PRIMARY KEY (upper_limit_id), 
	UNIQUE (jd, fid, pid, diffmaglim)
)

;
CREATE TABLE candidate (
	candidate_id SERIAL NOT NULL, 
	alert_id INTEGER NOT NULL, 
	jd DOUBLE PRECISION, 
	fid INTEGER, 
	pid BIGINT, 
	diffmaglim FLOAT, 
	programid INTEGER, 
	candid BIGINT, 
	isdiffpos BOOLEAN, 
	tblid BIGINT, 
	nid INTEGER, 
	rcid INTEGER, 
	field INTEGER, 
	xpos FLOAT, 
	ypos FLOAT, 
	ra DOUBLE PRECISION, 
	dec DOUBLE PRECISION, 
	magpsf FLOAT, 
	sigmapsf FLOAT, 
	chipsf FLOAT, 
	magap FLOAT, 
	sigmagap FLOAT, 
	distnr FLOAT, 
	magnr FLOAT, 
	sigmagnr FLOAT, 
	chinr FLOAT, 
	sharpnr FLOAT, 
	sky FLOAT, 
	magdiff FLOAT, 
	fwhm FLOAT, 
	classtar FLOAT, 
	mindtoedge FLOAT, 
	magfromlim FLOAT, 
	seeratio FLOAT, 
	aimage FLOAT, 
	bimage FLOAT, 
	aimagerat FLOAT, 
	bimagerat FLOAT, 
	elong FLOAT, 
	nneg INTEGER, 
	nbad INTEGER, 
	rb FLOAT, 
	ssdistnr FLOAT, 
	ssmagnr FLOAT, 
	sumrat FLOAT, 
	magapbig FLOAT, 
	sigmagapbig FLOAT, 
	ranr DOUBLE PRECISION, 
	decnr DOUBLE PRECISION, 
	sgmag1 FLOAT, 
	srmag1 FLOAT, 
	simag1 FLOAT, 
	szmag1 FLOAT, 
	sgscore1 FLOAT, 
	distpsnr1 FLOAT, 
	ndethist INTEGER, 
	ncovhist INTEGER, 
	jdstarthist DOUBLE PRECISION, 
	jdendhist DOUBLE PRECISION, 
	scorr DOUBLE PRECISION, 
	tooflag INTEGER, 
	objectidps1 BIGINT, 
	objectidps2 BIGINT, 
	sgmag2 FLOAT, 
	srmag2 FLOAT, 
	simag2 FLOAT, 
	szmag2 FLOAT, 
	sgscore2 FLOAT, 
	distpsnr2 FLOAT, 
	objectidps3 BIGINT, 
	sgmag3 FLOAT, 
	srmag3 FLOAT, 
	simag3 FLOAT, 
	szmag3 FLOAT, 
	sgscore3 FLOAT, 
	distpsnr3 FLOAT, 
	nmtchps INTEGER, 
	rfid BIGINT, 
	jdstartref DOUBLE PRECISION, 
	jdendref DOUBLE PRECISION, 
	nframesref INTEGER, 
	PRIMARY KEY (candidate_id), 
	UNIQUE (candid, programid, pid), 
	FOREIGN KEY(alert_id) REFERENCES alert (alert_id) ON DELETE CASCADE ON UPDATE CASCADE
)

;CREATE UNIQUE INDEX alert_id ON candidate (alert_id);
CREATE TABLE cutout (
	cutout_id SERIAL NOT NULL, 
	alert_id INTEGER NOT NULL, 
	kind cutout_kind NOT NULL, 
	"stampData" BYTEA NOT NULL, 
	PRIMARY KEY (cutout_id), 
	FOREIGN KEY(alert_id) REFERENCES alert (alert_id) ON DELETE CASCADE ON UPDATE CASCADE
)

;
CREATE TABLE alert_prv_candidate_pivot (
	alert_id INTEGER NOT NULL, 
	prv_candidate_id INTEGER[] NOT NULL, 
	PRIMARY KEY (alert_id), 
	FOREIGN KEY(alert_id) REFERENCES alert (alert_id) ON DELETE CASCADE ON UPDATE CASCADE
)

;
CREATE TABLE alert_upper_limit_pivot (
	alert_id INTEGER NOT NULL, 
	upper_limit_id INTEGER[] NOT NULL, 
	PRIMARY KEY (alert_id), 
	FOREIGN KEY(alert_id) REFERENCES alert (alert_id) ON DELETE CASCADE ON UPDATE CASCADE
)

;INSERT INTO versions (alert_version) VALUES (1.8);
