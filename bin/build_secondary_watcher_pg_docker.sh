#!/usr/bin/env bash
set -e

sbt store-pg/assembly
jarfile="$(ls -t store-pg/target/scala-2.*/store-pg-assembly-*.jar | head -1)"
cp "$jarfile" store-pg/docker/store-pg-assembly.jar
docker build --pull -t secondary-watcher store-pg/docker
