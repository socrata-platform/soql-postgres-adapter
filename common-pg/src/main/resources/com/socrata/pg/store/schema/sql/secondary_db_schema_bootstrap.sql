DO $$BEGIN

IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dataset_lifecycle_stage')) THEN
  CREATE TYPE dataset_lifecycle_stage AS ENUM('Unpublished', 'Published', 'Snapshotted', 'Discarded');
END IF;

-- Allows for a only single given row to have the value "Unit"
IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'unit')) THEN
  CREATE TYPE unit AS ENUM('Unit');
END IF;

CREATE TABLE IF NOT EXISTS dataset_map (
  -- Note: when flipping a backup to primary, the system_id sequence object must be setup since
  -- playing back logs doesn't access the object.
  system_id             BIGSERIAL                    NOT NULL PRIMARY KEY,
  next_counter_value    BIGINT                       NOT NULL,
  locale_name           VARCHAR(40)   NOT NULL,
  obfuscation_key       BYTEA                        NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_internal_name_map (
  dataset_internal_name VARCHAR(40)    NOT NULL, -- Will be unique across a given SODA server environment (i.e. Azure West)
  dataset_system_id BIGINT                           NOT NULL REFERENCES dataset_map(system_id),
  UNIQUE (dataset_internal_name)
);

CREATE TABLE IF NOT EXISTS copy_map (
  system_id             BIGSERIAL                  NOT NULL PRIMARY KEY,
  dataset_system_id     BIGINT                     NOT NULL REFERENCES dataset_map(system_id),
  copy_number           BIGINT                     NOT NULL, -- this gets incremented per copy made.
  lifecycle_stage       dataset_lifecycle_stage    NOT NULL,
  data_version          BIGINT                     NOT NULL, -- this refers to the log's version
  UNIQUE (dataset_system_id, copy_number)
);

CREATE TABLE IF NOT EXISTS column_map (
  system_id                 BIGINT                        NOT NULL,
  copy_system_id            BIGINT                        NOT NULL REFERENCES copy_map(system_id),
  user_column_id            VARCHAR(40) NOT NULL,
  type_name                 VARCHAR(40)      NOT NULL,
  -- all your physical columns are belong to us
  physical_column_base_base VARCHAR(40)   NOT NULL, -- the true PCB is p_c_b_b + "_" + system_id
  is_system_primary_key     unit                          NULL, -- evil "unique" hack
  is_user_primary_key       unit                          NULL, -- evil "unique" hack
  is_version                unit                          NULL, -- evil "unique" hack
  -- Making a copy preserves the system_id of columns.  Therefore, we need a two-part primary key
  -- in order to uniquely identify a column.
  -- It's in the order table-id-then-column-id so the implied index can (I think!) be used for
  -- "give me all the columns of this dataset version".
  PRIMARY KEY (copy_system_id, system_id),
  UNIQUE (copy_system_id, user_column_id),
  UNIQUE (copy_system_id, is_system_primary_key), -- hack hack hack
  UNIQUE (copy_system_id, is_user_primary_key), -- hack hack hack
  UNIQUE (copy_system_id, is_version) -- hack hack hack
);

CREATE TABLE IF NOT EXISTS pending_table_drops (
  id         BIGSERIAL                 NOT NULL PRIMARY KEY,
  table_name VARCHAR(80) NOT NULL,
  queued_at  TIMESTAMP WITH TIME ZONE  NOT NULL
);

END$$;
