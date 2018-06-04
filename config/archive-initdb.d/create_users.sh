#!/usr/bin/env bash

psql=( psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" )

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

file_env 'ARCHIVE_READ_USER'
file_env 'ARCHIVE_READ_USER_PASSWORD'

file_env 'ARCHIVE_WRITE_USER'
file_env 'ARCHIVE_WRITE_USER_PASSWORD'

"${psql[@]}" <<-EOSQL
	CREATE USER "$ARCHIVE_READ_USER" WITH PASSWORD '$ARCHIVE_READ_USER_PASSWORD';
	GRANT SELECT ON ALL TABLES IN SCHEMA public TO "$ARCHIVE_READ_USER";
	CREATE USER "$ARCHIVE_WRITE_USER" WITH PASSWORD '$ARCHIVE_WRITE_USER_PASSWORD';
	GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO "$ARCHIVE_WRITE_USER";
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "$ARCHIVE_WRITE_USER";
EOSQL
