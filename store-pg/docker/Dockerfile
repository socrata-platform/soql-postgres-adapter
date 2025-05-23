FROM socrata/java8-focal

# TODO expose JMX
ENV JMX_PORT 6300
EXPOSE $JMX_PORT

RUN apt-get -y update && apt-get -y install jq

ENV SERVER_ROOT /srv/secondary-watcher
ENV SERVER_ARTIFACT store-pg-assembly.jar
ENV SERVER_CONFIG secondary-watcher.conf

# general secondary-watcher defaults
ENV TRUTH_CLUSTER alpha
ENV TRUTH_CLUSTER_COLLOCATION_GROUP [alpha]
ENV ENABLE_GRAPHITE false
ENV GRAPHITE_HOST 0.0.0.0
ENV GRAPHITE_PORT 0
ENV JAVA_XMX 512m
ENV LOG_METRICS false
ENV DATA_COORDINATOR_DB_NAME truth
ENV DATA_COORDINATOR_DB_PORT 5432
ENV DATA_COORDINATOR_DB_USER soda_dc
ENV JAVA_MAX_METASPACE 64m
ENV EURYBATES_PRODUCERS activemq
ENV ZOOKEEPER_SESSION_TIMEOUT 4s

WORKDIR $SERVER_ROOT

COPY ship.d /etc/ship.d
COPY ${SERVER_CONFIG}.j2 $SERVER_ROOT/
COPY $SERVER_ARTIFACT $SERVER_ROOT/

ENV SECONDARY_CONFIG secondary.conf

# pg-secondary-specific defaults
ENV PG_SECONDARY_DB_NAME falth
ENV PG_SECONDARY_DB_PORT 5432
ENV PG_SECONDARY_DB_USER soda_store
ENV PG_SECONDARY_NUM_WORKERS 5
ENV PG_SECONDARY_WORK_MEM 768MB
ENV PG_SECONDARY_TABLESPACE_FN nil
ENV PG_SECONDARY_METRICS_ENABLED true

# A clojure expression which will be evaluated for each upsert in a
# context where the variables "inserts", "updates", "deletes",
# "columns", and "estimated-rows" are defined.  If it returns true,
# the secondary will make an indexless copy of the dataset, do the
# upsert on that dataset, then rebuild the indexes afterward.
ENV PG_SECONDARY_TRANSPARENT_COPY_FUNCTION (> (+ inserts updates deletes) (max 100000 (/ estimated-rows 3)))

WORKDIR $SERVER_ROOT

COPY ${SECONDARY_CONFIG}.j2 $SERVER_ROOT/
COPY $SECONDARY_ARTIFACT $SERVER_ROOT/
