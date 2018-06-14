
CREATE EXTENSION cube;
CREATE EXTENSION earthdistance;

/* redefine earth radius such that great circle distances are in degrees */
CREATE OR REPLACE FUNCTION earth() RETURNS float8
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS 'SELECT 180/pi()';

CREATE INDEX cone_search on candidate USING gist (ll_to_earth(dec, ra));
