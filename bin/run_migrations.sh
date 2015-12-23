#!/bin/bash
BASEDIR=$(dirname $0)/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}
JARFILE=$BASEDIR/store-pg/target/scala-2.10/store-pg-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
java -Dconfig.file=$CONFIG -cp $JARFILE com.socrata.pg.store.MigrateSchema Migrate