#!/usr/bin/env bash
set -e

sbt soql-server-pg/assembly
jarfile="soql-server-pg/target/soql-server-pg-assembly.jar"
cp "$jarfile" soql-server-pg/docker/soql-server-pg-assembly.jar
docker build --pull -t soql-server-pg soql-server-pg/docker
