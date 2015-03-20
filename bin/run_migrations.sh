#!/bin/bash
# Run migrations.  Pass name of database as first arg.
# For names, see the configs listed under com.socrata.pg.store
# As of 6/13/2014 this is "database" and "test-database".
if [ -z "$1" ]; then
  echo "Usage: $0 database|test-database"
  exit 1
fi
BASEDIR=$(dirname $0)/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}
JARFILE=$BASEDIR/store-pg/target/scala-2.10/store-pg-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
java -Dconfig.file=$CONFIG -cp $JARFILE com.socrata.pg.store.MigrateSchema Migrate com.socrata.pg.store.$1
