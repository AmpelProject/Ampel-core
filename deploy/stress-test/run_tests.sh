#!/bin/bash
set -e

# Create indices and insert config
ampel-init-db --host $MONGO || exit 1

ampel-init-archive --host $ARCHIVE || exit 1

# run tests
pytest /Ampel/test -vv -k archive -x

