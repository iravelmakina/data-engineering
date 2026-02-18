-- =============================================================
-- 02_parse.sql
-- Flatten raw_tweets_champions (nested JSON) into structured tables.
-- Run after 01_load.sql (requires: raw_tweets_champions table)
--
-- Output tables:
--   tweets         — one row per tweet, scalar + user fields flat
--   tweet_hashtags — one row per hashtag per tweet (unnested array)
-- =============================================================


-- -------------------------------------------------------------
-- TABLE 1: tweets
-- Extracts scalar fields and flattens the nested `user` STRUCT.
-- Pattern: "struct_col".field  (dot notation for STRUCT access)
-- tweet_date derived from timestamp_ms (Unix ms → DATE)
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE tweets AS
SELECT id_str                 AS tweet_id,
       timestamp_ms::BIGINT   AS timestamp_ms, to_timestamp(timestamp_ms::BIGINT / 1000)::DATE AS tweet_date, lang,
       retweet_count,
       favorite_count,
       reply_count,

       -- flatten nested user struct (STRUCT → individual columns)
       "user".id_str          AS user_id,
       "user".screen_name     AS user_screen_name,
       "user".followers_count AS followers_count,
       "user".verified        AS user_verified

FROM raw_tweets_champions;


-- -------------------------------------------------------------
-- TABLE 2: tweet_hashtags
-- Unnests entities.hashtags array — one row per hashtag per tweet.
-- Pattern: UNNEST(array_col) AS t(alias) — each element is a
-- STRUCT {text VARCHAR, indices INTEGER[]}, we extract .text only.
-- lang is carried over from the parent row for use in Insight 1.
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE tweet_hashtags AS
SELECT id_str  AS tweet_id,
       lang,
       ht.text AS hashtag
FROM raw_tweets_champions,
     UNNEST(entities.hashtags) AS t(ht)
WHERE ht.text IS NOT NULL;


-- -------------------------------------------------------------
-- Sanity checks
-- -------------------------------------------------------------
SELECT 'tweets' AS table_name, COUNT(*) AS rows
FROM tweets
UNION ALL
SELECT 'tweet_hashtags', COUNT(*)
FROM tweet_hashtags;
