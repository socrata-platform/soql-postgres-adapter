#!/usr/bin/env bash
set -e

image="$1"
if [ -z "$image" ]; then
  echo "Please pass in the image to run as the first argument to this script!"
  exit 1
fi

local_config_dir="$(dirname "$(realpath "$0")")/../configs"
docker run \
  -e JMX_PORT=6700 \
  -e SERVER_CONFIG="configs/application-charlie.conf" \
  -v "$local_config_dir":/srv/secondary-watcher/configs \
  -p 6700:6700 \
  -d -t "$image"
