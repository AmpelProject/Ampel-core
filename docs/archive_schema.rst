
Updating the archive database schema
====================================

The archive database stores IPAC avro alerts in nearly complete form so that
they can be reprocessed as filters and computations are added and changed in
Ampel. The avro format is however not fixed; IPAC can and does update the
schema at will. In order to catch such changes, the ArchiveDB client checks the
schema version in the avro packet and raises an error if it is newer than the
database schema. To get Ampel running again, the database schema needs to be
updated. This requires 3 steps.

1. Acquire an example of the new schema; either from the UW broker or IPAC
GitHub repo. Write it to a json file in alerts/schema_X.Y.json, where X.Y is
the version number, with `json.dump(schema, f, indent=1)`

2. Find differences between the old and new schemas, e.g. with `diff -u schema_2.0.json schema_X.Y.json`.

3. Using the diff as a guide, write a SQL migration script like the existing
ones in deploy/production/initdb/archive/*.sql, prefixing it with a number to
ensure that it is executed in order. For example, the 3.0 schema added fields
to the `candidate` dict that were not present in the 2.0 schema. The migration
script adds these fields to the `candidate` table, and updates the schema version::
    
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

4. Test the migration. The easiest way to do this is to write a test that uses
the `postgres` fixture defined in ampel-core, connects to the resulting
database, and ensures that a) `alert_version` is as expected, and b) the new
fields exist in the table. This works because the fixture mounts the
deploy/production/initdb/archive directory as /docker-initdb.d, causing postgres
to execute all SQL scripts in order after initialization.

5. Commit the migration script. Now, check out the release currently running in
production, cherry-pick the commit you just made to master, create a new
incremental tag, and push it.

6. Moment of truth: apply the migration. ssh to `transit.ifh.de`. Change to the
Ampel-core source directory, pull, and check out the tag you pushed in step 5.
Become user ampel with `sudo su ampel`. Run `singularity-stack` to find the
instance name of the archive container, e.g.::
    
    (singularity-stack) [transit] /home/ampel > singularity-stack list
    Stack                          Services                       Replicas Instance
    ============================== ============================== ======= ========
    transit                        archive                                280e9712
                                   graphite                               061fe0d6
    ------------------------------ ------------------------------ ------- --------
Now, execute the migration with e.g.::
    
    singularity exec instance://280e9712 psql ztfarchive /docker-entrypoint-initdb.d/MIGRATION.sql
where `MIGRATION.sql` is the name of the migration script you created in step 3.

7. Verify that alerts are running again by sshing to `burst.ifh.de`, becoming
ampel, observing the lack of scary error messages in the output of
`singularity-stack logs burst alertprocessor -f`.
