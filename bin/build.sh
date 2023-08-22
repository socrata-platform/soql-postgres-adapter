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
SERVER_JAR="soql-server-pg/target/soql-server-pg-assembly.jar"
WATCHER_JAR="store-pg/target/store-pg-assembly.jar"

if [ -f "$SERVER_JAR" ] && [ -f "$WATCHER_JAR" ]; then
    if [ "$SERVER_JAR" -ot "$WATCHER_JAR" ]; then
        OLDEST="$SERVER_JAR"
    else
        OLDEST="$WATCHER_JAR"
    fi
else
    OLDEST=""
fi

SRC_PATHS=($(find .  -maxdepth 2 -name 'src' -o -name '*.sbt' -o -name '*.scala'))
if [ -z "$OLDEST" ] || [ "$(find "${SRC_PATHS[@]}" -newer "$OLDEST")" ]; then
    if [ "$1" == '--fast' ]; then
        echo 'Assembly is out of date.  Fast start is enabled so skipping rebuild anyway...' >&2
    else
        nice -n 19 sbt assembly >&2
        touch "$SERVER_JAR" "$WATCHER_JAR"
    fi
fi

echo "soql-server-pg: $(python -c "import os; print(os.path.realpath('$SERVER_JAR'))")"
echo "store-pg: $(python -c "import os; print(os.path.realpath('$WATCHER_JAR'))")"

