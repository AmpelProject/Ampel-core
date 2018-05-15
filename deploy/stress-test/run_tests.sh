#!/bin/bash
set -e
/Ampel/run_stress_test.py --host $MONGO --archive-host $ARCHIVE --procs 32 /ztf/*.tar.gz.part*

