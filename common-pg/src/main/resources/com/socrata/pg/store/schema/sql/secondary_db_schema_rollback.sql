DO $$BEGIN

DROP TABLE IF EXISTS pending_table_drops;
DROP TABLE IF EXISTS column_map;
DROP TABLE IF EXISTS copy_map;
DROP TABLE IF EXISTS dataset_internal_name_map;
DROP TABLE IF EXISTS dataset_map;
DROP TYPE IF EXISTS unit;
DROP TYPE IF EXISTS dataset_lifecycle_stage;

END$$;
