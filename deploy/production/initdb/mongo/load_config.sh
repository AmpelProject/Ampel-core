#!/usr/bin/env bash

# Initialize configuration database in docker entrypoint

import=( mongoimport --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin -d Ampel_config )

cd  /docker-entrypoint-initdb.d
for f in *.json; do
	# upack object an array of objects with keys as _id
	cat $f | jq 'to_entries | map({_id: .key} + .value)' | ${import[@]} --collection $(basename $f .json) --jsonArray
done

file_env 'MONGO_USER'
file_env 'MONGO_PASSWORD'

mongo=( mongo --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin )

# munge into database names ino js objects
roles=$(echo '"Ampel Ampel_config Ampel_troubles"' | jq 'split(" ") | [{db : .[], role : "readWrite"}]')
"${mongo[@]}" "$rootAuthDatabase" <<-EOJS
	db.createUser({
		user: $(_js_escape "$MONGO_USER"),
		pwd: $(_js_escape "$MONGO_PASSWORD"),
		roles: $roles
	})
	db.grantRolesToUser($(_js_escape "$MONGO_USER"), [{"role": "clusterMonitor", "db": "admin"}])
EOJS
