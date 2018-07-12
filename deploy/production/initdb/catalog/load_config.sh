#!/usr/bin/env bash

# Initialize configuration database in docker entrypoint

file_env 'MONGO_USER'
file_env 'MONGO_PASSWORD'

# The ToO user has same privs as read user, but can also write to the ToO db
file_env 'TOO_USER'
file_env 'TOO_PASSWORD'

if [[ ! -d "$MONGODUMP_DIR" ]]; then
	echo "MONGODUMP_DIR ($MONGODUMP_DIR) is not a directory!"
	exit 1
elif [[ $(find $MONGODUMP_DIR -maxdepth 0 -dtype d -empty 2>/dev/null) ]]; then
	echo "MONGODUMP_DIR ($MONGODUMP_DIR) is empty!"
fi

# build a quoted string of catalog names
catalogs=\"$(find $MONGODUMP_DIR/ -maxdepth 1 -mindepth 1 -type d -exec basename {} \; | xargs echo)\"
# munge into list of js objects
roles=$(echo $catalogs | jq 'split(" ") | [{db : .[], role : "read"}]')

mongo=( mongo --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin )
"${mongo[@]}" "$rootAuthDatabase" <<-EOJS
	db.runCommand({ createRole: "listDatabases",
		privileges: [
			{ resource: { cluster : true }, actions: ["listDatabases"]}
			],
		roles: []
	})
	db.createUser({
		user: $(_js_escape "$MONGO_USER"),
		pwd: $(_js_escape "$MONGO_PASSWORD"),
		roles: $roles
	})
	db.grantRolesToUser($(_js_escape "$MONGO_USER"), [{"role": "listDatabases", "db": "admin"}])
	db.createUser({
		user: $(_js_escape "$TOO_USER"),
		pwd: $(_js_escape "$TOO_PASSWORD"),
		roles: $roles
	})
	db.grantRolesToUser($(_js_escape "$TOO_USER"), [{"role": "listDatabases", "db": "admin"}, {"role": "readWrite", "db": "ToO"}])
EOJS

restore=( mongorestore --host 127.0.0.1 --port 27017 --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin )
${restore[@]} $MONGODUMP_DIR
