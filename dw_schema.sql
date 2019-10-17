CREATE TABLE IF NOT EXISTS staging (
    id SERIAL PRIMARY KEY
  , load_id_str VARCHAR(36)
  , load_id UUID
  , load_time TIMESTAMP WITH TIME ZONE
  , report_date  TIMESTAMP WITH TIME ZONE
  , local_timezone VARCHAR(64)
  , sheriff VARCHAR(128)
  , count_population_total INTEGER
  , count_population_total_men INTEGER
  , count_population_total_women INTEGER
  , count_population_sentenced_felony_men INTEGER
  , count_population_sentenced_felony_women INTEGER
  , count_population_sentenced_misdemeanor_men INTEGER
  , count_population_sentenced_misdemeanor_women INTEGER
  , count_population_unsentenced_felony_men INTEGER
  , count_population_unsentenced_felony_women INTEGER
  , count_population_unsentenced_misdemeanor_men INTEGER
  , count_population_unsentenced_misdemeanor_women INTEGER
)
;
CREATE INDEX IF NOT EXISTS idx_staging_1
  ON staging (load_time)
;

/*
JDBC does not have a type for uuid. This trigger will populate
the load_id from load_id_str and set the load_id_str to null
*/

CREATE OR REPLACE FUNCTION set_load_id()
  RETURNS trigger AS $set_load_id$
BEGIN
IF NEW.LOAD_ID IS NULL THEN
NEW.LOAD_ID = CAST(NEW.LOAD_ID_STR AS UUID);
END IF;
NEW.LOAD_ID_STR = NULL;
RETURN NEW;
END;
$set_load_id$ LANGUAGE 'plpgsql'
;

CREATE TRIGGER set_load_id BEFORE INSERT
  ON staging
  FOR EACH ROW
  EXECUTE FUNCTION set_load_id()
;

CREATE TABLE IF NOT EXISTS dim_date (
    id SERIAL PRIMARY KEY
  , full_date DATE NOT NULL
  , year_no INTEGER NOT NULL
  , month_no INTEGER NOT NULL
  , day_no INTEGER NOT NULL
  , weekday_no INTEGER NOT NULL
  , day_no_in_year INTEGER NOT NULL
  , quarter_no INTEGER NOT NULL
  , week_no INTEGER NOT NULL
  , week_year INTEGER NOT NULL
  , iso_week VARCHAR(7)
  , is_weekday BOOLEAN NOT NULL
)
;

CREATE INDEX IF NOT EXISTS idx_dim_date_1
  ON dim_date(full_date)
;

CREATE TABLE dim_sheriff (
    id SERIAL PRIMARY KEY
  , sheriff VARCHAR(128)
)
;

CREATE TABLE IF NOT EXISTS fact_jail_population_count (
    id SERIAL PRIMARY KEY
  , report_date_id INTEGER NOT NULL
  , sheriff_id INTEGER
  , load_id UUID
  , count_population_total INTEGER
  , count_population_total_men INTEGER
  , count_population_total_women INTEGER
  , count_population_sentenced_felony_men INTEGER
  , count_population_sentenced_felony_women INTEGER
  , count_population_sentenced_misdemeanor_men INTEGER
  , count_population_sentenced_misdemeanor_women INTEGER
  , count_population_unsentenced_felony_men INTEGER
  , count_population_unsentenced_felony_women INTEGER
  , count_population_unsentenced_misdemeanor_men INTEGER
  , count_population_unsentenced_misdemeanor_women INTEGER
)
;