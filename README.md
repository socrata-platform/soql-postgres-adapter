soql-postgres-adapter
=====================

Postgres Adapter for SODAServer

### Build Requirements
sbt
webdav4sbt - available at https://bitbucket.org/diversit/webdav4sbt.git

### Build

sudo -u postgres createdb -O blist -E utf-8 secondary_test
createdb -O blist -E utf-8 secondary_test

java -Dconfig=/etc/soda2.conf -cp store-pg/target/scala-2.10/store-pg-assembly-0.0.19-SNAPSHOT.jar com.socrata.pg.store.MigrateSchema migrate database
java -Dconfig=/etc/soda2.conf -cp store-pg/target/scala-2.10/store-pg-assembly-0.0.19-SNAPSHOT.jar com.socrata.pg.store.MigrateSchema migrate test-database

sbt test package assembly
