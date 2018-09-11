
Odds and ends
*************

Add a new extcats catalog
=========================

1. Punch a hole in the firewall for `rsync` from ztf-wgs::
    
    ssh -R5000:localhost:22 burst

2. Become user ampel and sync catalogs::
    
    sudo su ampel
    cd /data/ampel/catalogs
    rsync -avz --progress -e 'ssh -p 5000' jvsanten@localhost:/lustre/fs19/group/cta/users/mgiomi/mongodumps .

3. Restore catalogs and update roles::
    
    SINGULARITYENV_MONGO_INITDB_ROOT_PASSWORD=`cat ~/dryrun/secrets/mongo-root-password.txt` SINGULARITYENV_MONGO_INITDB_ROOT_USERNAME=root singularity-stack exec burst catalog /docker-entrypoint-initdb.d/add_catalog.sh $CATALOG_NAME
  
  where `CATALOG_NAME` is the name of the catalog you just rsynced.

Add a new catsHTM catalog
=========================

1. Punch a hole in the firewall for `rsync` from ztf-wgs::
    
    ssh -R5000:localhost:22 burst

2. Become user ampel and sync catalogs::
    
    sudo su ampel
    cd /data/ampel/catalogs
    rsync -avz --progress -e 'ssh -p 5000' jvsanten@localhost:/lustre/fs19/group/cta/users/mgiomi/catsHTM2 .

Export target catalog for cross-checks
======================================

On burst: (replace `af186630` with the container id of the catalog service, and `$PASSWORD` with the catalog read password)::
  
  singularity shell instance://af186630
  cd /docker-entrypoint-initdb.d/
  mongoexport --port 27018 --username filterclient --password $PASSWORD --authenticationDatabase admin --db ToO --collection neutrinos --jsonArray -o neutrinos.json