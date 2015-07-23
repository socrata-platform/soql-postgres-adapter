# Docker support

The files in this directory allow you to build a docker image.  The soql-server-pg assembly must be
copied to `soql-server-pg-assembly.jar` in this directory before building.

## Required Runtime Variables

* `PG_SECONDARY_DB_HOST` - Data Coordinator DB hostname
* `PG_SECONDARY_DB_PASSWORD_LINE` - Full line of config for soql-server-pg DB password.  Designed to be either `password = "foo"` or `include /path/to/file`.
* `ZOOKEEPER_ENSEMBLE` - The zookeeper cluster to talk to, in the form of `["10.0.0.1:2181", "10.0.0.2:2818"]`

## Optional Runtime Variables

See the Dockerfile for defaults.

* `ARK_HOST` - The IP address of the host of the docker container, used for service advertisements.
* `ENABLE_GRAPHITE` - Should various metrics information be reported to graphite
* `GRAPHITE_HOST` - The hostname or IP of the graphite server, if enabled
* `GRAPHITE_PORT` - The port number for the graphite server, if enabled
* `JAVA_XMX` - Sets the -Xmx and -Xms parameters to control the JVM heap size
* `LOG_METRICS` - Should various metrics information be logged to the log
* `PG_SECONDARY_DB_NAME` - soql-server-pg DB database name
* `PG_SECONDARY_DB_PORT` - soql-server-pg DB port number
* `PG_SECONDARY_DB_USER` - soql-server-pg DB user name
* `PG_SECONDARY_INSTANCE` - soql-server-pg instance name
* `PG_SECONDARY_WORK_MEM` - The postgres `work_mem` setting to use for updating the secondary.  This is primarily useful for allowing PostgreSQL to do expensive queries such as large group by queries.
