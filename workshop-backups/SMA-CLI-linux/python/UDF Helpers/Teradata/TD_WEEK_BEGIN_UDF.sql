-- <copyright file="TD_WEEK_BEGIN_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.TD_WEEK_BEGIN_UDF(
    input_date DATE,
    mode STRING DEFAULT 'TERADATA'
)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  WITH week_start AS (
    SELECT
      -- DATE inputs always use Sunday-first week start (simpler Teradata behavior)
      input_date - DAYOFWEEK(input_date) AS week_begin_date
    )
    SELECT week_begin_date FROM week_start
$$;

-- TIMESTAMP_NTZ input version - implements mode-specific logic + sets time to 20:00:00 (Teradata behavior)
CREATE OR REPLACE FUNCTION PUBLIC.TD_WEEK_BEGIN_UDF(
    input_timestamp TIMESTAMP_NTZ,
    mode STRING DEFAULT 'TERADATA'
)
RETURNS TIMESTAMP_NTZ
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  WITH timestamp_week_start AS (
    SELECT
      CASE
        WHEN UPPER(mode) = 'ISO' THEN
          -- ISO mode in Teradata: Week starts on SUNDAY (not Monday as in ISO 8601!)
          input_timestamp::DATE - DAYOFWEEK(input_timestamp::DATE)
        WHEN UPPER(mode) = 'COMPATIBLE' THEN
          -- COMPATIBLE mode: Same as TERADATA mode (Saturday start)
          input_timestamp::DATE - DAYOFWEEK(input_timestamp::DATE) - 1
        WHEN UPPER(mode) = 'TERADATA' THEN
          -- TERADATA mode: Week starts on SATURDAY (not Sunday!)
          input_timestamp::DATE - DAYOFWEEK(input_timestamp::DATE) - 1
        ELSE
          -- Default to TERADATA mode (Saturday start)
          input_timestamp::DATE - DAYOFWEEK(input_timestamp::DATE) - 1
      END AS week_begin_date
  )
  SELECT
    DATEADD(HOUR, 20, week_begin_date::TIMESTAMP_NTZ)
  FROM timestamp_week_start
$$;