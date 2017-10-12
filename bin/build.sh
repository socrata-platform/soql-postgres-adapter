#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

PIDFILE="${BASEDIR}/.build.sh.pid"

SBT_OPTS=(-Xmx1024M ${SBT_OPTS})

if [ -f "${PIDFILE}" ] && ps "$(head -n 1 "${PIDFILE}")" >/dev/null 2>/dev/null; then
    echo 'An artifact build appears to already be running.' >&2
    echo >&2
    echo -n 'Waiting for it to finish...' >&2


    while [ -f "${PIDFILE}" ] && ps "$(head -n 1 "${PIDFILE}")" >/dev/null 2>/dev/null; do
        echo -n '.' >&2
        sleep 5
    done

    echo ' done.' >&2
fi

PID=$$
echo "${PID}" > "${PIDFILE}"

export SBT_OPTS="-Xmx2048M"

cd "$BASEDIR"
SERVER_JAR="$(ls -rt soql-server-pg/target/scala-*/soql-server-pg-assembly*.jar 2>/dev/null | tail -n 1)"
WATCHER_JAR="$(ls -rt store-pg/target/scala-*/store-pg-assembly*.jar 2>/dev/null | tail -n 1)"

if [ -n "$SERVER_JAR" ] \
       && [ -n "$WATCHER_JAR" ]; then
    OLDEST="$(ls -rt "$SERVER_JAR" "$WATCHER_JAR" | head -n 1)"
fi

SRC_PATHS=($(find .  -maxdepth 2 -name 'src' -o -name '*.sbt' -o -name '*.scala'))
if [ -z "$OLDEST" ] || [ "$(find "${SRC_PATHS[@]}" -newer "$OLDEST")" ]; then
    if [ "$1" == '--fast' ]; then
        echo 'Assembly is out of date.  Fast start is enabled so skipping rebuild anyway...' >&2
    else
        nice -n 19 sbt assembly >&2

        SERVER_JAR="$(ls -rt soql-server-pg/target/scala-*/soql-server-pg-assembly*.jar 2>/dev/null | tail -n 1)"
        WATCHER_JAR="$(ls -rt store-pg/target/scala-*/store-pg-assembly*.jar 2>/dev/null | tail -n 1)"

        touch "$SERVER_JAR" "$WATCHER_JAR"
    fi
fi

echo "soql-server-pg: $(python -c "import os; print(os.path.realpath('$SERVER_JAR'))")"
echo "store-pg: $(python -c "import os; print(os.path.realpath('$WATCHER_JAR'))")"

