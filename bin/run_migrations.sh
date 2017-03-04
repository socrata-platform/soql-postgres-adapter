#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

psql datacoordinator -c "insert into secondary_stores_config (store_id, next_run_time, interval_in_seconds) values ('pg', now(), 5) on conflict do nothing"

CONFIG=${SODA_CONFIG:-/etc/pg-secondary.conf}

JARFILE=$("$BINDIR"/build.sh "$@" | grep '^store-pg: ' | sed 's/^store-pg: //')

ARGS=(Migrate)
java -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.pg.store.Main --migrate "${ARGS[@]}"
