#!/bin/bash
BASEDIR=$(dirname "$0")/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}

JARS=( $BASEDIR/store-pg/target/scala-*/store-pg-assembly-*.jar )
# shellcheck disable=SC2012
JARFILE=$(ls -t "${JARS[@]}" | head -n 1)

if [ ! -e "$JARFILE" ]; then
    cd "$BASEDIR" && sbt assembly
    JARS=( $BASEDIR/store-pg/target/scala-*/store-pg-assembly-*.jar )
    # shellcheck disable=SC2012
    JARFILE=$(ls -t "${JARS[@]}" | head -n 1)
fi
java -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.pg.store.MigrateSchema Migrate
