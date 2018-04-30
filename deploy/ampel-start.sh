#!/bin/bash

# Create indices and insert config
ampel-init-db --host $MONGO || exit 1

ampel-init-archive --host $ARCHIVE || exit 1

# For lack of something better to do, spin through a tarball of alerts
ampel-alertprocessor --host $MONGO --archive-host $ARCHIVE --broker epyc.astro.washington.edu:9092 --procs 16 --chunksize 5000

