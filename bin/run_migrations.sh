#!/bin/bash
BASEDIR=$(dirname "$0")/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/pg-secondary.conf}

JARS=( $BASEDIR/store-pg/target/scala-2.10/store-pg-assembly-*.jar )
# shellcheck disable=SC2012
JARFILE=$(ls -t "${JARS[@]}" | head -n 1)

if [ ! -e "$JARFILE" ]; then
    cd "$BASEDIR" && sbt assembly
    JARS=( $BASEDIR/store-pg/target/scala-2.10/store-pg-assembly-*.jar )
    # shellcheck disable=SC2012
    JARFILE=$(ls -t "${JARS[@]}" | head -n 1)
fi
ARGS=(Migrate)
java -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.pg.store.Main --migrate "${ARGS[@]}"
