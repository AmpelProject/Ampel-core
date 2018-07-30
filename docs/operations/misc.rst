
Odds and ends
*************

Export target catalog for cross-checks
======================================

On burst: (replace `af186630` with the container id of the catalog service, and `$PASSWORD` with the catalog read password)::
  
  singularity shell instance://af186630
  cd /docker-entrypoint-initdb.d/
  mongoexport --port 27018 --username filterclient --password $PASSWORD --authenticationDatabase admin --db ToO --collection neutrinos --jsonArray -o neutrinos.json