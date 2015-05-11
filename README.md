soql-postgres-adapter
=====================

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

## Running the service

For active development, when you always want the latest up to date code in your repo, you will probably be executing this from an SBT shell:

    soql-postgres-adapter/run

For running the soql-postgres-adapter as one of several microservices, it might
be better to build the assembly and run it to save on memory:

    bin/start_pg_adapter.sh

## Run the Database migrations

To run the migrations:
```sh
sbt -Dconfig.file=/etc/soda2.conf "run-main com.socrata.pg.store.MigrateSchema migrate"
```

Alternatively, if you have an assembly jar you can use:
```sh
bin/run_migrations.sh
```

Running from sbt is recommended in a development environment because
it ensures you are running the latest migrations without having to build a
new assembly.

