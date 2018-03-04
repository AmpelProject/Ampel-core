#!/bin/bash

if [ -z "$1" ]; then
	echo "Please provide an output folder path as argument"
	exit
fi

mkdir -p $1

for c in `mongo localhost/Ampel_config --quiet --eval "db.getCollectionNames()" | tr -d ',[]"'`
do
	mkdir -p $1/$c
    mongoexport -d Ampel_config -c $c -o "$1/$c/config.json" --pretty --jsonArray
done
