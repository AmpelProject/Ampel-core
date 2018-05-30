#!/bin/sh
set -e
# stupid hack: redirect subshell fds to parent shell
exec 3>&1
exec 4>&2
ls /ztf/*.tar* | xargs -Ibla -P10 -n1 /Ampel/xargs_stress_test.py --host $MONGO bla >&3 2>&4
