#!/usr/bin/env bash

# Initialize configuration database in docker entrypoint

import=( mongoimport --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin -d Ampel_config --file )

cd  /docker-entrypoint-initdb.d
for f in *.json; do
	${import[@]} $f
done

file_env 'MONGO_USER'
file_env 'MONGO_PASSWORD'

mongo=( mongo --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin )

"${mongo[@]}" "$rootAuthDatabase" <<-EOJS
	db.createUser({
		user: $(_js_escape "$MONGO_USER"),
		pwd: $(_js_escape "$MONGO_PASSWORD"),
		roles: [ { role: 'readWrite', db: $(_js_escape "Ampel") } ]
	})
EOJS
