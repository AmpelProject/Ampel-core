#!/bin/bash
set -e

# Create indices and insert config
ampel-init-db --host $MONGO --config /Ampel/config/test/**/*.json || exit 1

ampel-init-archive --host $ARCHIVE || exit 1

# i drank your milkshake! i draaaaaank it up!
/Ampel/ingest_archives.py --host $MONGO --archive-host $ARCHIVE --procs 16 /ztf/*.tar.gz

