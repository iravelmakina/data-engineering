-- =============================================================
-- 04_quality.sql
-- Data quality checks on parsed tables.
-- Run after 02_parse.sql (requires: tweets, tweet_hashtags)
-- Covers: null counts, duplicates, schema/value validation
-- =============================================================


-- -------------------------------------------------------------
-- 1. NULL COUNTS — tweets table (all columns)
-- Identifies missing values per column.
-- Critical columns (tweet_id, timestamp_ms) should have 0 nulls.
-- -------------------------------------------------------------
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(*) FILTER (WHERE tweet_id IS NULL)            AS null_tweet_id,
    COUNT(*) FILTER (WHERE timestamp_ms IS NULL)        AS null_timestamp,
    COUNT(*) FILTER (WHERE tweet_date IS NULL)          AS null_tweet_date,
    COUNT(*) FILTER (WHERE lang IS NULL)                AS null_lang,
    COUNT(*) FILTER (WHERE retweet_count IS NULL)       AS null_retweet_count,
    COUNT(*) FILTER (WHERE favorite_count IS NULL)      AS null_favorite_count,
    COUNT(*) FILTER (WHERE reply_count IS NULL)         AS null_reply_count,
    COUNT(*) FILTER (WHERE user_id IS NULL)             AS null_user_id,
    COUNT(*) FILTER (WHERE user_screen_name IS NULL)    AS null_user_screen_name,
    COUNT(*) FILTER (WHERE followers_count IS NULL)     AS null_followers_count,
    COUNT(*) FILTER (WHERE user_verified IS NULL)       AS null_user_verified
FROM tweets;


-- -------------------------------------------------------------
-- 2. DUPLICATE CHECK — tweets table
-- Each tweet_id should appear exactly once.
-- -------------------------------------------------------------
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT tweet_id)        AS unique_tweet_ids,
    COUNT(*) - COUNT(DISTINCT tweet_id) AS duplicate_rows
FROM tweets;


-- -------------------------------------------------------------
-- 3. DUPLICATE CHECK — tweet_hashtags table
-- Same (tweet_id, hashtag) pair should not appear more than once.
-- -------------------------------------------------------------
SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(DISTINCT tweet_id || '|' || hashtag)      AS unique_pairs,
    COUNT(*) - COUNT(DISTINCT tweet_id || '|' || hashtag) AS duplicate_pairs
FROM tweet_hashtags;


-- -------------------------------------------------------------
-- 4. VALUE VALIDATION — numeric range checks
-- retweet_count, favorite_count, reply_count must be >= 0.
-- followers_count must be >= 0.
-- -------------------------------------------------------------
SELECT
    COUNT(*) FILTER (WHERE retweet_count < 0)   AS negative_retweets,
    COUNT(*) FILTER (WHERE favorite_count < 0)  AS negative_favorites,
    COUNT(*) FILTER (WHERE reply_count < 0)     AS negative_replies,
    COUNT(*) FILTER (WHERE followers_count < 0) AS negative_followers
FROM tweets;


-- -------------------------------------------------------------
-- 5. SCHEMA VALIDATION — timestamp sanity
-- All tweets should fall within the known capture window:
-- 2018-05-26 (match day). Flag any out-of-range dates.
-- -------------------------------------------------------------
SELECT
    MIN(tweet_date) AS earliest_date,
    MAX(tweet_date) AS latest_date,
    COUNT(*) FILTER (WHERE tweet_date <> '2018-05-26') AS out_of_range_dates
FROM tweets;


-- -------------------------------------------------------------
-- 6. LANGUAGE DISTRIBUTION
-- Shows how many tweets per language — helps spot unexpected
-- or suspicious language codes.
-- -------------------------------------------------------------
SELECT
    lang,
    COUNT(*)                                        AS tweet_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM tweets
GROUP BY lang
ORDER BY tweet_count DESC;
