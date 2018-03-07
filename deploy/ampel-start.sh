#!/bin/bash

source activate ampel 2>/dev/null

# For the moment, provision a new conda environment from scratch if one is not
# present. In the final release this will be frozen into the image as an entry
# point.
if [[ $? -ne 0 ]]; then
	conda create --yes -n ampel --file Ampel/requirements.txt python=3 --channel=conda-forge
	source activate ampel
	pip install -e Ampel
fi

# Clear out __pycache__ to prevent conflicts between conda envs inside and
# outside the container
find Ampel \( -iname \*.pyc -o -iname \*.pyo \) -delete

# Create indices and insert config
ampel-init-db --host $MONGO || exit 1

ampel-init-archive --host $ARCHIVE || exit 1

# For lack of something better to do, spin through a tarball of alerts
ampel-alertprocessor --host $MONGO --archive-host $ARCHIVE /ztf/test_alerts.tar.gz --procs 10 --chunksize 5000
