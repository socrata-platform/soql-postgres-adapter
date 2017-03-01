#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG=${SODA_CONFIG:-/etc/pg-secondary.conf}

JARFILE=$("$BINDIR"/build.sh "$@" | grep '^store-pg: ' | sed 's/^store-pg: //')

ARGS=(Migrate)
java -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.pg.store.Main --migrate "${ARGS[@]}"
