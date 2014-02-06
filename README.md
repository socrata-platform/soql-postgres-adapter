soql-postgres-adapter
=====================

Postgres Adapter for SODAServer

### Build Requirements
sbt
webdav4sbt - available at https://bitbucket.org/diversit/webdav4sbt.git

### Build

sudo -u postgres createdb -O blist -E utf-8 secondary_test
createdb -O blist -E utf-8 secondary_test
sbt test package assembly

