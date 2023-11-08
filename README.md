soql-postgres-adapter
=====================

This repository is the home to the following:
- soql-postgres-adapter: adapter code for translating SoQL statements to Postgres-compliant SQL statements
- secondary-watcher-pg: the secondary (ie. the write-side) of the Postgres dataset query backend
- soql-server-pg: an HTTP server that takes SoQL queries from query-coordinator, rewrites them as SQL queries, and sends them to Postgres

Postgres Adapter for SODAServer

## Build Requirements
sbt
webdav4sbt - available at https://bitbucket.org/diversit/webdav4sbt.git

## Build and Test

```sh
sudo -u postgres createdb -O blist -E utf-8 secondary_test
sudo -u postgres createdb -O blist -E utf-8 secondary
createdb -O blist -E utf-8 secondary_test
createdb -O blist -E utf-8 secondary
sbt test package assembly
```

## Testing redshift functionality

If the [reference.conf](https://github.com/socrata-platform/soql-postgres-adapter/blob/dalia%2Fredshift_SOQL/common-pg/src/test/resources/reference.conf#L1) specifies a redshift DB (via the db-type), then the Redshift tests will attempt to run the tests against redshift as well verify that SOQL produces the appropriate SQL.
If the [reference.conf](https://github.com/socrata-platform/soql-postgres-adapter/blob/dalia%2Fredshift_SOQL/common-pg/src/test/resources/reference.conf#L1) does *not* specify a redshift DB, then the Redshift tests will *not* attempt to run the tests against redshift and will only verify that SOQL produces the appropriate SQL.

## Running the service

### soql-server-pg

For active development, when you always want the latest up to date code in your repo, run with SBT:

    sbt -Dconfig.file=configs/application.conf soql-server-pg/run

For running soql-server-pg as one of several microservices, it might
be better to build the assembly and run it to save on memory:

    bin/start_soql_server_pg.sh

### secondary-watcher-pg

For active development, when you always want the latest up to date code in your repo, run with SBT:

    sbt -Dconfig.file=configs/application.conf store-pg/run

For running secondary-watcher-pg as one of several microservices, it might
be better to build the assembly and run it to save on memory:

    bin/start_secondary_watcher_pg.sh

If configured, secondary-watcher-pg can send messages to Eurybates when replication for its group (`"read"`) has completed. To configure this locally add to your secondary-watcher config:

```
message-producer {
    eurybates {
      producers = "activemq"
      activemq.connection-string = "tcp://localhost:61616"
    }
    zookeeper {
      conn-spec = "localhost:2181"
      session-timeout = 4s
    }
  }
```

## Run the Database migrations

To run the migrations:
```sh
sbt -Dconfig.file=/etc/pg-secondary.conf "run-main com.socrata.pg.store.Main --migrate migrate"
```

Alternatively, if you have an assembly jar you can use:
```sh
bin/run_migrations.sh
```

Running from sbt is recommended in a development environment because
it ensures you are running the latest migrations without having to build a
new assembly.
