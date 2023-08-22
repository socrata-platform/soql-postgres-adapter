#!/usr/bin/env bash
set -e

sbt store-pg/assembly
jarfile="store-pg/target/store-pg-assembly.jar"
cp "$jarfile" store-pg/docker/store-pg-assembly.jar
docker build --pull -t secondary-watcher-pg store-pg/docker
