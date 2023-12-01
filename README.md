soql-postgres-adapter
=====================

This repository is home to 3 subprojects:
- store-pg: code for our Postgres secondary-watcher application; the secondary-watcher is the write-side of the Postgres dataset query backend
- soql-server-pg: an HTTP server application that accepts requests as SoQL queries and rewrites them as SQL queries, and sends them to Postgres
- common-pg: common code shared between the above two applications

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
