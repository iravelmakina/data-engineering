-- =============================================================
-- 01_load.sql
-- Load raw JSON into DuckDB staging table.
-- Dataset: TweetsChampions.json — tweets captured during the
-- 2018 UEFA Champions League Final via Twitter Streaming API.
-- =============================================================

-- Set your local path here before running.
-- SET VARIABLE (not SET) is required for user-defined variables in DuckDB.
SET VARIABLE tweets_path = '01_assignment/TweetsChampions.json';

-- sample_size = -1 forces DuckDB to scan the entire file before
-- inferring column types, preventing errors from rare fields
-- (e.g. withheld_in_countries) that appear only in later records.
CREATE OR REPLACE TABLE raw_tweets_champions AS
SELECT *
FROM read_json_auto(getvariable('tweets_path'), sample_size = -1);

-- Sanity check
SELECT COUNT(*) AS total_records
FROM raw_tweets_champions;
