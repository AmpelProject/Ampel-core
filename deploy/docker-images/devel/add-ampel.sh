#!/bin/bash

# Install Ampel in editable mode. Only entry points and the egg link are
# written to the container; the source directory must be mounted at /Ampel 
# again at runtime. This is not super robust, but allows for rapid iteration
# with read-only containers (e.g. Singularity)

# Find absolute path to Ampel source dir
DIR=$(echo "${0%/*}")
AMPELDIR=$(cd "$DIR/../../.." && echo "$(pwd -L)")

container=$(docker run --user root -it -d -v $AMPELDIR:/Ampel ampel sh)
docker exec -t $container pip install -e /Ampel
status=$?
docker stop $container
docker wait $container

if [[ $status -eq 0 ]]; then
	docker commit $container ampel
fi
