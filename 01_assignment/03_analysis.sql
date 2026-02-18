-- =============================================================
-- 03_analysis.sql
-- Window function analysis on TweetsChampions dataset.
-- Run after 02_parse.sql (requires: tweets, tweet_hashtags tables)
-- =============================================================


-- -------------------------------------------------------------
-- INSIGHT 1: Top 3 hashtags per language
--
-- What it shows: which hashtags dominate in each language community
-- during the 2018 UCL Final. Reveals how different language groups
-- engaged with the event (e.g. Spanish fans using #HalaMadrid,
-- English fans using #LFC).
--
-- Window function: RANK() OVER (PARTITION BY lang ORDER BY uses DESC)
-- Partitions hashtag counts by language, ranks within each partition.
-- -------------------------------------------------------------
WITH hashtag_counts AS (SELECT lang,
                               UPPER(hashtag) AS hashtag,
                               COUNT(*)       AS uses
                        FROM tweet_hashtags
                        GROUP BY lang, UPPER(hashtag)),
     ranked AS (SELECT lang,
                       hashtag,
                       uses,
                       RANK() OVER (
            PARTITION BY lang
            ORDER BY uses DESC
        ) AS rank_in_lang
                FROM hashtag_counts)
SELECT UPPER(lang) AS lang,
       rank_in_lang,
       hashtag,
       uses
FROM ranked
WHERE rank_in_lang <= 3
  AND lang IS NOT NULL
ORDER BY lang, rank_in_lang;


-- -------------------------------------------------------------
-- INSIGHT 2: Running total of tweets per hour on match day
--
-- What it shows: how tweet activity built up hour by hour during
-- the 2018 UCL Final (2018-05-26). The stream was captured from
-- 18:45 to 20:45 UTC (kickoff to final whistle), so the hourly
-- running total shows the match rhythm: pre-match → first half
-- → second half → full time.
--
-- Window function: SUM() OVER (ORDER BY tweet_hour
--                              ROWS BETWEEN UNBOUNDED PRECEDING
--                              AND CURRENT ROW)
-- Computes cumulative tweet count across hours of match day.
-- -------------------------------------------------------------
WITH hourly_counts AS (SELECT date_trunc('hour', to_timestamp(timestamp_ms / 1000)) AS tweet_hour_utc,
                              COUNT(*)                                              AS tweets_in_hour
                       FROM tweets
                       WHERE timestamp_ms IS NOT NULL
                       GROUP BY date_trunc('hour', to_timestamp(timestamp_ms / 1000)))
SELECT tweet_hour_utc,
       tweet_hour_utc + INTERVAL '3 hours' AS tweet_hour_kyiv, tweets_in_hour, SUM (tweets_in_hour) OVER (
    ORDER BY tweet_hour_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM hourly_counts
ORDER BY tweet_hour_utc;
