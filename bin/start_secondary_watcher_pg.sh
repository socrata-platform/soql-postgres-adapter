#!/bin/bash
set -e

# Starts the soql-pg-adapter service.
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG=$1
if [[ -z $CONFIG ]]; then
  CONFIG="$BINDIR/../configs/application.conf"
fi

JARFILE=$("$BINDIR"/build.sh "$@" | grep '^store-pg: ' | sed 's/^store-pg: //')

# TODO: should pass the config down to run migrations...
# but I don't actually need to for what I am trying to do here
"$BINDIR"/run_migrations.sh

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE"
