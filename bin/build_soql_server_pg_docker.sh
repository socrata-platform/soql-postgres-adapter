#!/usr/bin/env bash
set -e

#sbt soql-server-pg/assembly
jarfile="$(ls -t soql-server-pg/target/scala-2.*/soql-server-pg-assembly-*.jar | head -1)"
cp "$jarfile" soql-server-pg/docker/soql-server-pg-assembly.jar
docker build --pull -t soql-server-pg soql-server-pg/docker
