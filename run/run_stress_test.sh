#!/bin/sh
set -e
# stupid hack: redirect subshell fds to parent shell
exec 3>&1
exec 4>&2

echo "Starting statpublisher"
python3 /Ampel/run/run_t0_stats_publisher.py &
LASTPID=$!

ls /ztf/*.tar* | xargs -Ibla -P10 -n1 /Ampel/run/run_on_single_tar.py --host $MONGO bla >&3 2>&4

echo "Stopping statpublisher"
wait $LASTPID
stil_running=`ps -ef | grep $LASTPID`
if [ ! -z "$still_running" ]; then echo "Sending SIGKILL" && kill -9 $LASTPID; fi
echo "Done"
