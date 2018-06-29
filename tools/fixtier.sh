#!/bin/sh

# fixtier.sh <blob_path>
# This script is a wrapper of fixtier.exe. It should be
# placed in the same directory.

DIR="$( cd "$( dirname "$0" )" && pwd )"
SCRIPT="$DIR/fixtier.exe"
MONO="/usr/local/bin/mono"

mkdir -p /tmp/fixtier

printf "`date` -------------------\n" >> /tmp/fixtier/log
$MONO $SCRIPT \
    --connection-string='_________________' \
    --container='_____________' \
    --blob-path="$1" 2>&1 >> /tmp/fixtier/log

ret=$?
if [ $ret -ne 0 ]; then
    printf "`date`,$1\n" >> /tmp/fixtier/error.csv
fi

exit $ret

