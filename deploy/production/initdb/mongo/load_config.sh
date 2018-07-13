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

file_env 'LOGGER_USER'
file_env 'LOGGER_PASSWORD'

mongo=( mongo --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin )

# writer: needs _readWrite_ on Ampel_data, Ampel_reports and _read_ on Ampel_logs
# logger: needs _readWrite_ on Ampel_logs

# munge into database names ino js objects
"${mongo[@]}" "$rootAuthDatabase" <<-EOJS
	db.createUser({
		user: $(_js_escape "$MONGO_USER"),
		pwd: $(_js_escape "$MONGO_PASSWORD"),
		roles: $(echo '"Ampel_data"' | jq 'split(" ") | [{db : .[], role : "readWrite"}]')
	})
	db.grantRolesToUser($(_js_escape "$MONGO_USER"), [{"role": "read", "db": "Ampel_logs"}])
	db.createUser({
		user: $(_js_escape "$LOGGER_USER"),
		pwd: $(_js_escape "$LOGGER_PASSWORD"),
		roles: $(echo '"Ampel_logs Ampel_reports"' | jq 'split(" ") | [{db : .[], role : "readWrite"}]')
	})
	db.grantRolesToUser($(_js_escape "$LOGGER_USER"), [{"role": "clusterMonitor", "db": "admin"}])
	db.grantRolesToUser($(_js_escape "$LOGGER_USER"), [{"role": "read", "db": "Ampel_data"}])
EOJS
