#!/bin/bash

set -euxo pipefail

root=${1:-.}

# find all directories containing Python files, and run mypy on them
find $root -name test -prune -o -type f -name '*.py' -printf "%h\n" | sort -u | while read dir; do
    pkg=$(echo $dir | sed -e 's|/|.|g')
    (mypy $dir | mypy2junit) > mypy.$pkg.xml
done
