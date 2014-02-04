CREATE TABLE IF NOT EXISTS dataset_map (
  -- Note: when flipping a backup to primary, the system_id sequence object must be set since
  -- playing back logs doesn't access the object.
  system_id          BIGSERIAL                    NOT NULL PRIMARY KEY,
  internal_name      VARCHAR(%TABLE_NAME_LEN%)    NOT NULL,
  next_counter_value BIGINT                       NOT NULL,
  locale_name        VARCHAR(%LOCALE_NAME_LEN%)   NOT NULL,
  obfuscation_key    BYTEA                        NOT NULL
);
