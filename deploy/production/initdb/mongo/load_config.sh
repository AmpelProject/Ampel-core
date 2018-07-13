#!/usr/bin/env bash

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
