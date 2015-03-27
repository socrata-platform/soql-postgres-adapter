# Secondary Watcher docker support

The files in this directory allow you to build a docker image for the PG secondary watcher.  
The store-pg assembly must be copied to `store-pg-assembly.jar` in this directory before building.

## Required Runtime Variables

All variables required by [the data-coordinator secondary watcher base image](https://github.com/socrata/data-coordinator/tree/master/coordinator/docker-secondary-watcher#required-runtime-variables)
are required.  

In addition, the following are required:

* `PG_SECONDARY_DB_HOST` - PG Secondary DB hostname
* `PG_SECONDARY_INSTANCE` - The secondary instance to talk to.  Must match DB_HOST or madness ensues.  TODO: support more than one
* `PG_SECONDARY_DB_PASSWORD_LINE` - Full line of config for soql-server-pg DB password.  Designed to be either `password = "foo"` or `include /path/to/file`.

## Optional Runtime Variables

All optional variables supported by [the data-coordinator secondary watcher base image](https://github.com/socrata/data-coordinator/tree/master/coordinator/docker-secondary-watcher#optional-runtime-variables)
are supported.  

In addition, the following optional variables are supported.  For defaults, see the [Dockerfile](Dockerfile).

* `LOG_METRICS` - Should various metrics information be logged to the log
* `PG_SECONDARY_DB_NAME` - soql-server-pg DB database name
* `PG_SECONDARY_DB_PORT` - soql-server-pg DB port number
* `PG_SECONDARY_DB_USER` - soql-server-pg DB user name
* `PG_SECONDARY_INSTANCE` - soql-server-pg instance name
* `PG_SECONDARY_NUM_WORKERS` - Number of workers to run per secondary instance.
* `PG_SECONDARY_TABLESPACE_FN` - A Clojure function used to generate the tablespace name for datasets.
