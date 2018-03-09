#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

$BINDIR/start_secondary_watcher_pg.sh "$BINDIR/../configs/application-charlie.conf"
