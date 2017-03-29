#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

psql datacoordinator -c "insert into secondary_stores_config (store_id, next_run_time, interval_in_seconds) SELECT * FROM (SELECT 'pg'::text as store_id, now(), 5) t WHERE store_id NOT IN (SELECT store_id FROM secondary_stores_config)"

CONFIG=${SODA_CONFIG:-/etc/pg-secondary.conf}

JARFILE=$("$BINDIR"/build.sh "$@" | grep '^store-pg: ' | sed 's/^store-pg: //')

ARGS=(Migrate)
java -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.pg.store.Main --migrate "${ARGS[@]}"
