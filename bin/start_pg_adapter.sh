#!/bin/bash
# Starts the soql-pg-adapter service.
BASEDIR=$(dirname $0)/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}
JARFILE=$BASEDIR/soql-server-pg/target/scala-2.10/soql-server-pg-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
java -Dconfig.file=$CONFIG -jar $JARFILE &
